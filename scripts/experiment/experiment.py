import argparse,subprocess,metadata,graphs
from kazoo.client import KazooClient
from kazoo.recipe.barrier import Barrier
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType

class Experiment(object):
  def __init__(self,conf,run_id):
    self.conf=conf
    self.run_id=run_id
    self._zk=KazooClient(hosts=metadata.zk)
    self._zk.start()
    
  def run(self):
    #read in configuration
    print("\n\n\nParsing configuration file")
    self.parse()

    #kill existing routing service, broker and monitoring processes
    print("\n\n\nKilling all existing rb,eb,rs,monitor and client processes")
    self.clean()

    #clean zk tree
    print("\n\n\nCleaning up zk tree")
    self.clean_zk_tree()

    #clear logs
    print("\n\n\nCleaning log directory on all remote hosts")
    self.clean_logs()
    
    #register zk listeners for this experiment run
    print("\n\n\nSetting up zk tree for coordination of test processes")
    self.setup_zk_coordination()

    #launch routing service, broker and monitoring processes    
    print("\n\n\nStarting rs,rb,eb,monitors and ptpd on infrastructure nodes")
    self.setup_infrastructure()

    #launch subscribers
    print("\n\n\nStarting subscriber processes")
    self.start_subscribers()

    #wait for all subscribers to join
    print("\n\n\nWaiting on subscriber barrier, until all subscribers have joined")
    self.sub_barrier.wait()

    #launch publishers
    print("Starting publisher processes")
    self.start_publishers()

    #wait for experiment to finish 
    print("\n\n\nWaiting on finished barrier, until all subscribers have exited")
    self.finished_barrier.wait()

    #wait for all monitoring process to exit
    print("\n\n\nWaiting on monitoring barrier, until all monitors have exited")
    self.monitoring_barrier.wait()

    #collect logs
    print("\n\n\nCollecting logs")
    self.collect_logs()

    #create graphs
    print("\n\n\nCreating graphs")
    self.create_graphs()

    #exit
    self._zk.stop()

  def parse(self):
    with open(self.conf) as f:
      for line in f:
        if line.startswith('rbs'):
          self.rbs=line.rstrip().partition(':')[2].split(',')
          print('rbs:'+str(self.rbs))
        elif line.startswith('ebs'):
          self.ebs=line.rstrip().partition(':')[2].split(',')
          print('ebs:'+str(self.ebs))
        elif line.startswith('clients'):
          self.clients=line.rstrip().partition(':')[2].split(',')
          print('clients:'+str(self.clients))
        elif line.startswith('topics'):
          self.topics=line.rstrip().partition(':')[2].split(',')
          self.no_topics=len(self.topics)
          print('topics:'+str(self.topics))
        elif line.startswith('no_subs'):
          self.no_subscribers=int(line.rstrip().partition(':')[2])
          print('no_subs:'+str(self.no_subscribers))
        elif line.startswith('no_pubs'):
          self.no_publishers=int(line.rstrip().partition(':')[2])
          print('no_pubs:'+str(self.no_publishers))
        elif line.startswith('sub_distribution'):
          self.subscribers={}
          for sub_description in line.rstrip().partition(':')[2].split(','):
            host,topic,num_sub= sub_description.split(':')
            if host in self.subscribers:
              self.subscribers[host].update({topic: num_sub})
            else:
              self.subscribers[host]={topic: num_sub}
          print('subscribers:'+str(self.subscribers))
        elif line.startswith('pub_distribution'):
          self.publishers={}  
          for pub_description in line.rstrip().partition(':')[2].split(','):
            host,topic,num_pub= pub_description.split(':')
            if host in self.publishers:
              self.publishers[host].update({topic: num_pub})
            else:
              self.publishers[host]={topic: num_pub}
          print('publishers:'+str(self.publishers))
        elif line.startswith('sample_count'):
          self.sample_count=int(line.rstrip().partition(':')[2])
          print('sample_count:'+str(self.sample_count))
        elif line.startswith('sleep_interval'):
          self.sleep_interval=int(line.rstrip().partition(':')[2])
          print('sleep_interval:'+str(self.sleep_interval))
        else:
          print('invalid line')
    self.brokers= ','.join(self.ebs)+','+','.join(self.rbs)

  def clean(self):
    #kill existing Broker processes on brokers
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=kill pattern=Broker"'%(metadata.ansible,self.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing routing service processes on brokers
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=kill pattern=rtirouting"'%(metadata.ansible,self.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing monitoring processes on brokers
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=kill pattern=Monitor"'%(metadata.ansible,self.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #clean shared memory resources and semaphores on brokers
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=ipcsrm"'%(metadata.ansible,self.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing publishers and subscriber processes on clients
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=kill pattern=pubsubcoord.clients.Client"'%\
      (metadata.ansible,','.join(self.clients))
    subprocess.check_call(['bash','-c', command_string])

    #clean shared memory resources and semaphores on clients
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=ipcsrm"'%(metadata.ansible,','.join(self.clients))
    subprocess.check_call(['bash','-c', command_string])

  def clean_zk_tree(self):
    if self._zk.exists(metadata.topics_path):
      self._zk.delete(metadata.topics_path,recursive=True)

    if self._zk.exists(metadata.leader_path):
      self._zk.delete(metadata.leader_path,recursive=True)

    if self._zk.exists(metadata.rb_path):
      self._zk.delete(metadata.rb_path,recursive=True)

    if self._zk.exists(metadata.experiment_path):
      self._zk.delete(metadata.experiment_path,recursive=True)

  def clean_logs(self):
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=clean_logs"'%(metadata.ansible,self.brokers)
    subprocess.check_call(['bash','-c',command_string])

    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=clean_logs"'%(metadata.ansible,','.join(self.clients))
    subprocess.check_call(['bash','-c',command_string])

  def setup_infrastructure(self):
    #start routing service on all brokers
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=start_rs"'%(metadata.ansible,self.brokers)
    subprocess.check_call(['bash','-c',command_string])

    #start RoutingBroker on rbs
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=start_rb emulated_broker=%d"'%\
      (metadata.ansible,','.join(self.rbs),1)
    subprocess.check_call(['bash','-c',command_string])

    #start monitoring process on rbs
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=start_monitor broker_type=rb run_id=%s"'%\
      (metadata.ansible,','.join(self.rbs),self.run_id)
    subprocess.check_call(['bash','-c',command_string])

    #start EdgeBroker on ebs
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=start_eb emulated_broker=%d"'%\
      (metadata.ansible,','.join(self.ebs),1)
    subprocess.check_call(['bash','-c',command_string])

    #start monitoring process on ebs
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=start_monitor broker_type=eb run_id=%s"'%\
      (metadata.ansible,','.join(self.ebs),self.run_id)
    subprocess.check_call(['bash','-c',command_string])

    #ensure ptpd is runing on rbs, ebs and clients
    #command_string='cd %s && ansible-playbook cluster.yml --limit %s\
    #  --extra-vars="action=start_ptpd"'%(metadata.ansible,','.join(self.rbs))
    #subprocess.check_call(['bash','-c',command_string])

    #command_string='cd %s && ansible-playbook cluster.yml --limit %s\
    #  --extra-vars="action=start_ptpd"'%(metadata.ansible,','.join(self.ebs))
    #subprocess.check_call(['bash','-c',command_string])

    #command_string='cd %s && ansible-playbook cluster.yml --limit %s\
    #  --extra-vars="action=start_ptpd"'%(metadata.ansible,','.join(self.clients))
    #subprocess.check_call(['bash','-c',command_string])


  def setup_zk_coordination(self):

    sub_path='%s/%s/sub'%(metadata.experiment_path,self.run_id)
    pub_path='%s/%s/pub'%(metadata.experiment_path,self.run_id)
    monitoring_path='%s/%s/monitoring'%(metadata.experiment_path,self.run_id)

    sub_barrier_path='%s/%s/barriers/sub'%(metadata.experiment_path,self.run_id)
    pub_barrier_path='%s/%s/barriers/pub'%(metadata.experiment_path,self.run_id)
    finished_barrier_path='%s/%s/barriers/finished'%(metadata.experiment_path,self.run_id)
    monitoring_barrier_path='%s/%s/barriers/monitoring'%(metadata.experiment_path,self.run_id)
    
    self._zk.ensure_path(sub_path)
    self._zk.ensure_path(pub_path)
    self._zk.ensure_path(monitoring_path)

    self._zk.ensure_path(sub_barrier_path)
    self._zk.ensure_path(pub_barrier_path)
    self._zk.ensure_path(finished_barrier_path)
    self._zk.ensure_path(monitoring_barrier_path)

    self.sub_barrier=Barrier(client=self._zk,path=sub_barrier_path)
    self.finished_barrier=Barrier(client=self._zk,path=finished_barrier_path)
    self.pub_barrier=Barrier(client=self._zk,path=pub_barrier_path)
    self.monitoring_barrier=Barrier(client=self._zk,path=monitoring_barrier_path)

    sub_barrier_open=False
    finished_barrier_open=False
    pub_barrier_open=False
    monitoring_barrier_open=False
    def _barrier_listener(children,event):
      nonlocal sub_barrier_open,pub_barrier_open,finished_barrier_open,monitoring_barrier_open
      if event and event.type==EventType.CHILD:
        if event.path==sub_path and len(children)==self.no_subscribers:
          print("all subscribers have joined. opening subscriber barrier")
          self.sub_barrier.remove()
          sub_barrier_open=True
        elif event.path==pub_path and len(children)==self.no_publishers:
          print("all publishers have joined. opening publisher barrier")
          self.pub_barrier.remove()
          pub_barrier_open=True
        elif event.path==sub_path and len(children)==0:
          print("all subscribers have exited. opening finished barrier")
          self.finished_barrier.remove()
          finished_barrier_open=True
        elif event.path==monitoring_path and len(children)==0:
          print("all monitors have exited. opening monitoring barrier")
          self.monitoring_barrier.remove()
          monitoring_barrier_open=True
        else:
          pass
        if all([sub_barrier_open,pub_barrier_open,finished_barrier_open,monitoring_barrier_open]):
          return False
        
    ChildrenWatch(client=self._zk,path=sub_path,func=_barrier_listener,send_event=True)
    ChildrenWatch(client=self._zk,path=pub_path,func=_barrier_listener,send_event=True)
    ChildrenWatch(client=self._zk,path=monitoring_path,func=_barrier_listener,send_event=True)
   
  def start_subscribers(self):
    for host,topic_count_map in self.subscribers.items():
      for topic,count in topic_count_map.items():
        command_string='cd %s && ansible-playbook cluster.yml --limit %s\
          --extra-vars="action=start_sub subscriber_count=%s topic=%s sample_count=%d run_id=%s"'%\
          (metadata.ansible,host,count,topic,self.sample_count,self.run_id)
        subprocess.check_call(['bash','-c',command_string])

  def start_publishers(self):
    for host, topic_count_map in self.publishers.items():
      for topic,count in topic_count_map.items():
        command_string='cd %s && ansible-playbook cluster.yml --limit %s\
          --extra-vars="action=start_pub publisher_count=%s topic=%s sample_count=%d sleep_interval=%d run_id=%s"'%\
          (metadata.ansible,host,count,topic,self.sample_count,self.sleep_interval,self.run_id)
        subprocess.check_call(['bash','-c',command_string])

  def collect_logs(self):
    #fetch logs from all brokers
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=fetch run_id=%s"'%(metadata.ansible,self.brokers,self.run_id)
    subprocess.check_call(['bash','-c',command_string])

    #fetch logs from all clients
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=fetch run_id=%s"'%\
      (metadata.ansible,','.join(self.clients),self.run_id)
    subprocess.check_call(['bash','-c',command_string])
    
  def create_graphs(self):
    test_dir='%s/logs/%s'%(metadata.ansible,self.run_id)
    graphs.broker_cpumem_vs_time(test_dir,'rb')
    graphs.broker_cpumem_vs_time(test_dir,'eb')
    graphs.per_sample_latency(test_dir,self.no_topics)  
    graphs.summary_statistics(test_dir,self.no_topics)

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting pubsubcoord experiment')
  parser.add_argument('conf',help='configuration file containing experiment setup information')
  parser.add_argument('run_id',type=int,help='run id of this experiment')
  args=parser.parse_args()

  Experiment(args.conf,args.run_id).run()
