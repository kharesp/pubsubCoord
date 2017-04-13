import argparse,subprocess,metadata,time,os,conf,zk,json
from plots import process_topics
from kazoo.recipe.barrier import Barrier
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType

class Experiment(object):
  def __init__(self,conf_file,run_id,kill):
    #load the test configuration
    self.conf=conf.Conf(conf_file)
    self.run_id=run_id
    #flag to determine whether to kill prexisting client endpoints
    self.kill=kill
    #zk communication
    self.zk=zk.Zk(self.run_id,self.conf)
   
    
  def run(self):
    #clean up
    self.clean()

    #register zk listeners for this experiment run
    print("\n\n\nSetting up zk tree for coordination of test processes")
    self.zk.setup()

    #launch monitoring processes on EBs and RBs
    print("\n\n\nStarting Monitoring processes on EBs and RBs")
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
    #kill existing monitoring and client processes
    if(self.kill):
      print("\n\n\nKilling all monitoring and client processes")
      self.kill_existing_processes()

    #clean zk tree
    print("\n\n\nCleaning up zk tree: /experiment and /topics path")
    self.zk.clean()

    #clear logs
    print("\n\n\nCleaning log directory on all clients and brokers")
    self.clean_logs()
    
    
  def kill_existing_processes(self):
    #kill existing Monitoring processes on brokers
    command_string='cd %s && ansible-playbook playbooks/experiment/kill.yml  --limit %s\
      --extra-vars="pattern=Monitor"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c', command_string])

    #kill existing publishers and subscriber processes on clients
    command_string='cd %s && ansible-playbook playbooks/experiment/kill.yml  --limit %s\
      --extra-vars="pattern=pubsubcoord.clients.Client"'%\
      (metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c', command_string])


  def clean_logs(self):
    #clean experiment logs from all host machines
    command_string='cd %s && ansible-playbook playbooks/experiment/clean.yml\
      --extra-vars="log_dir=/home/ubuntu/log/"  --limit %s'\
      %(metadata.ansible,self.conf.hosts)
    subprocess.check_call(['bash','-c',command_string])

  def setup_infrastructure(self):
    #ensure netem rules are set on clients
    #command_string='cd %s && ansible-playbook playbooks/experiment/netem_cli.yml  --limit %s'%\
    #  (metadata.ansible,','.join(self.conf.clients))
    #subprocess.check_call(['bash','-c',command_string])

    #delete netem rules  on clients
    command_string='cd %s && ansible-playbook playbooks/experiment/delete_netem.yml  --limit %s'%\
      (metadata.ansible,','.join(self.conf.clients))
    subprocess.check_call(['bash','-c',command_string])

    #start monitoring process on ebs
    command_string='cd %s && ansible-playbook playbooks/experiment/monitoring.yml  --limit %s\
      --extra-vars="broker_type=eb run_id=%s zk_connector=%s"'%\
      (metadata.ansible,','.join(self.conf.ebs),self.run_id,metadata.zk)
    subprocess.check_call(['bash','-c',command_string])

    #start monitoring process on rbs
    command_string='cd %s && ansible-playbook playbooks/experiment/monitoring.yml  --limit %s\
      --extra-vars="broker_type=rb run_id=%s zk_connector=%s"'%\
      (metadata.ansible,','.join(self.conf.rbs),self.run_id,metadata.zk)
    subprocess.check_call(['bash','-c',command_string])
   
  def start_subscribers(self):
    parallelism=10
    sorted_hosts=sorted(self.conf.subscribers.keys())
    num=len(sorted_hosts)//parallelism
    rem=len(sorted_hosts)%parallelism
    for i in range(num):
      pairs={k:self.conf.subscribers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
      rep=json.dumps(pairs)
      subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.run_id])
    
    pairs={k:self.conf.subscribers[k] for k in sorted_hosts[num*parallelism:]} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.run_id])

  def start_publishers(self):
    parallelism=10
    sorted_hosts=sorted(self.conf.publishers.keys())
    num=len(sorted_hosts)//parallelism
    rem=len(sorted_hosts)%parallelism
    for i in range(num):
      pairs={k:self.conf.publishers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
      rep=json.dumps(pairs)
      subprocess.check_call(['python','src/start_publishers.py',rep,str(self.conf.pub_sample_count),str(self.conf.sleep_interval),self.run_id])
    
    pairs={k:self.conf.publishers[k] for k in sorted_hosts[num*parallelism:]} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_publishers.py',rep,str(self.conf.pub_sample_count),str(self.conf.sleep_interval),self.run_id])

  def collect_logs(self):
    #ensure local log directory exists
    log_dir='%s/logs/%s'%(metadata.ansible,self.run_id)
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)

    #fetch logs from all hosts
    command_string='cd %s && ansible-playbook playbooks/experiment/copy.yml  --limit %s\
      --extra-vars="run_id=%s"'%(metadata.ansible,self.conf.hosts,self.run_id)
    subprocess.check_call(['bash','-c',command_string])
    
  def create_graphs(self):
    test_dir='%s/logs/%s'%(metadata.ansible,self.run_id)
    process_topics.process_topic_files(test_dir,self.conf.no_topics)
    #process_topics.plot_latency_per_topic(test_dir,self.conf.no_topics)
    #process_brokers.process_routing_service_files(test_dir,'rb')
    #process_brokers.process_routing_service_files(test_dir,'eb')
    #copy conf file to logs dir
    command_string='cp conf/conf.csv %s/logs/%s'%(metadata.ansible,self.run_id)
    subprocess.check_call(['bash','-c',command_string])

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting pubsubcoord experiment')
  parser.add_argument('conf',help='configuration file containing experiment setup information')
  parser.add_argument('run_id',help='run id of this experiment')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to specify whether to kill pre-existing client and monitoring processes')
  args=parser.parse_args()

  Experiment(args.conf,args.run_id,args.kill).clean()
