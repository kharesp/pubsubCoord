import argparse,subprocess,metadata,time,os,conf,zk
import process_topics,process_brokers
from kazoo.client import KazooClient
from kazoo.recipe.barrier import Barrier
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType

class Experiment(object):
  def __init__(self,conf_file,run_id):
    self.conf=conf.Conf(conf_file)
    self.run_id=run_id
    self.zk=zk.Zk(self.run_id,self.conf)
    
  def run(self):
    #clean up
    self.clean()

    #register zk listeners for this experiment run
    print("\n\n\nSetting up zk tree for coordination of test processes")
    self.zk.setup()

    #launch routing service, broker and monitoring processes    
    print("\n\n\nStarting rs,rb,eb,monitors and ptpd on infrastructure nodes")
    self.setup_infrastructure()

    #launch subscribers
    print("\n\n\nStarting subscriber processes")
    self.start_subscribers()

    #wait for all subscribers to join
    print("\n\n\nWaiting on subscriber barrier, until all subscribers have joined")
    self.zk.wait('subscriber')

    #launch publishers
    print("Starting publisher processes")
    self.start_publishers()

    #wait for experiment to finish 
    print("\n\n\nWaiting on finished barrier, until all subscribers have exited")
    self.zk.wait('finished')

    #wait for all monitoring process to exit
    print("\n\n\nWaiting on monitoring barrier, until all monitors have exited")
    self.zk.wait('monitoring')

    #collect logs
    print("\n\n\nCollecting logs")
    self.collect_logs()

    #create graphs
    print("\n\n\nCreating graphs")
    self.create_graphs()

    #exit
    self.zk.stop()


  def clean(self):
    #kill existing routing service, broker and monitoring processes
    print("\n\n\nKilling all existing rb,eb,rs,monitor and client processes")
    self.kill_existing_processes()

    #cleanup shmem and sem resources
    #print("\n\n\nCleaning shmem and semaphores on brokers and clients")
    #self.ipcsrm()

    #clean zk tree
    print("\n\n\nCleaning up zk tree")
    self.zk.clean()

    #clear logs
    print("\n\n\nCleaning log directory on all remote hosts")
    self.clean_logs()
    


  def ipcsrm(self):
    #clean shared memory resources and semaphores on clients
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=ipcsrm"'%(metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c', command_string])

    #clean shared memory resources and semaphores on brokers
    command_string='cd %s && ansible-playbook cluster.yml --limit %s\
      --extra-vars="action=ipcsrm"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c', command_string])
    
  def kill_existing_processes(self):
    #kill existing Broker processes on brokers
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=kill pattern=Broker"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing routing service processes on brokers
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=kill pattern=rtirouting"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing monitoring processes on brokers
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=kill pattern=Monitor"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing publishers and subscriber processes on clients
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=kill pattern=pubsubcoord.clients.Client"'%\
      (metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c', command_string])


  def clean_logs(self):
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=clean_logs"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c',command_string])

    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=clean_logs"'%(metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c',command_string])

  def setup_infrastructure(self):
    #ensure netem rules are set on clients
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=netem_client"'%(metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c',command_string])

    #ensure netem rules are set on rbs 
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=netem_rb"'%(metadata.ansible,','.join(self.conf.rbs))
    subprocess.check_call(['bash','-c',command_string])

    #start routing service on all brokers
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=start_rs"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c',command_string])

    #start RoutingBroker on rbs
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=start_rb emulated_broker=%d"'%\
      (metadata.ansible,','.join(self.conf.rbs),1)
    subprocess.check_call(['bash','-c',command_string])

    #start monitoring process on rbs
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=start_monitor broker_type=rb run_id=%s"'%\
      (metadata.ansible,','.join(self.conf.rbs),self.run_id)
    subprocess.check_call(['bash','-c',command_string])

    #start EdgeBroker on ebs
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=start_eb emulated_broker=%d"'%\
      (metadata.ansible,','.join(self.conf.ebs),1)
    subprocess.check_call(['bash','-c',command_string])

    #start monitoring process on ebs
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=start_monitor broker_type=eb run_id=%s"'%\
      (metadata.ansible,','.join(self.conf.ebs),self.run_id)
    subprocess.check_call(['bash','-c',command_string])

    #ensure ptpd is runing on rbs, ebs and clients
    #command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
    #  --extra-vars="action=start_ptpd"'%(metadata.ansible,','.join(self.conf.brokers))
    #subprocess.check_call(['bash','-c',command_string])

    #command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
    #  --extra-vars="action=start_ptpd"'%(metadata.ansible,','.join(self.conf.clients))
    #subprocess.check_call(['bash','-c',command_string])

   
  def start_subscribers(self):
    for host in sorted(self.conf.subscribers.keys()):
      topic_count_map= self.conf.subscribers[host]
      for topic,count in topic_count_map.items():
        command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
          --extra-vars="action=start_sub subscriber_count=%s topic=%s sample_count=%d run_id=%s"'%\
          (metadata.ansible,host,count,topic,self.conf.sample_count,self.run_id)
        subprocess.check_call(['bash','-c',command_string])

  def start_publishers(self):
    for host in sorted(self.conf.publishers.keys()):
      topic_count_map= self.conf.publishers[host]
      for topic,count in topic_count_map.items():
        command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
          --extra-vars="action=start_pub publisher_count=%s topic=%s sample_count=%d sleep_interval=%d run_id=%s"'%\
          (metadata.ansible,host,count,topic,self.conf.sample_count,self.conf.sleep_interval,self.run_id)
        subprocess.check_call(['bash','-c',command_string])

  def collect_logs(self):
    #ensure local log directory exists
    log_dir='%s/logs/%s'%(metadata.ansible,self.run_id)
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)

    #fetch logs from all brokers
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=fetch run_id=%s"'%(metadata.ansible,self.conf.brokers,self.run_id)
    subprocess.check_call(['bash','-c',command_string])

    #fetch logs from all clients
    command_string='cd %s && ansible-playbook cluster.yml  --limit %s\
      --extra-vars="action=fetch run_id=%s"'%\
      (metadata.ansible,','.join(self.conf.clients),self.run_id)
    subprocess.check_call(['bash','-c',command_string])
    
  def create_graphs(self):
    test_dir='%s/logs/%s'%(metadata.ansible,self.run_id)
    process_topics.process_topic_files(test_dir,self.conf.no_topics)
    process_topics.plot_latency_per_topic(test_dir,self.conf.no_topics)
    process_brokers.process_routing_service_files(test_dir,'rb')
    process_brokers.process_routing_service_files(test_dir,'eb')
    #copy conf file to logs dir
    command_string='cp conf/conf.csv %s/logs/%s'%(metadata.ansible,self.run_id)
    subprocess.check_call(['bash','-c',command_string])

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting pubsubcoord experiment')
  parser.add_argument('conf',help='configuration file containing experiment setup information')
  parser.add_argument('run_id',help='run id of this experiment')
  args=parser.parse_args()

  Experiment(args.conf,args.run_id).create_graphs()
