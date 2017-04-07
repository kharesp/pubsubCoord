import argparse,metadata,conf,subprocess
from kazoo.client import KazooClient

class Infrastructure(object):
  def __init__(self,conf_file,teardown):
    self.teardown=teardown
    self.conf=conf.Conf(conf_file)
    self._zk=KazooClient(hosts=metadata.zk)
    self._zk.start()
    
  def setup(self):
    #clean up
    if(self.teardown):
      self.clean()

    #set up zk paths
    print("\n\n\nCreating zk paths:/topics,/routingBrokers,/leader")
    self.setup_zk()

    #launch routing service, broker processes
    print("\n\n\nStarting rs,rb,eb on infrastructure nodes")
    self.setup_infrastructure()

    #exit
    self._zk.stop()


  def clean(self):
    #kill existing routing service, broker processes
    print("\n\n\nKilling all existing rb,eb,rs processes on brokers")
    self.kill_existing_processes()

    #clean zk tree
    print("\n\n\nCleaning up zk tree: rb, topics and leader path")
    self.delete_zk_path(metadata.topics_path)
    self.delete_zk_path(metadata.leader_path)
    self.delete_zk_path(metadata.rb_path)

    #clear logs
    print("\n\n\nCleaning log directory on all brokers")
    self.clean_logs()

  def kill_existing_processes(self):
    #kill existing Broker and RTI routing service processes on brokers
    command_string='cd %s && ansible-playbook playbooks/experiment/kill.yml  --limit %s\
      --extra-vars="pattern=Broker,rtirouting"'%(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c', command_string])

  def delete_zk_path(self,path):
    if self._zk.exists(path):
      self._zk.delete(path,recursive=True)

  def clean_logs(self):
    command_string='cd %s && ansible-playbook playbooks/experiment/clean.yml \
      --extra-vars="log_dir=/home/ubuntu/infrastructure_log/" --limit %s'\
      %(metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c',command_string])

  def setup_zk(self):
    self._zk.ensure_path(metadata.leader_path)
    self._zk.ensure_path(metadata.rb_path)
    self._zk.ensure_path(metadata.topics_path)

  def setup_infrastructure(self):
    #clean shmem resources on routing brokers
    #command_string='cd %s && ansible-playbook playbooks/experiment/shmem.yml  --limit %s'%\
    #  (metadata.ansible,','.join(self.conf.rbs))
    #subprocess.check_call(['bash','-c',command_string])

    #disable multicast on routing brokers
    command_string='cd %s && ansible-playbook playbooks/experiment/disable_multicast.yml  --limit %s'%\
      (metadata.ansible,','.join(self.conf.rbs))
    subprocess.check_call(['bash','-c',command_string])
    
    #ensure netem rules are set on rbs 
    command_string='cd %s && ansible-playbook playbooks/experiment/netem_rb.yml  --limit %s'%\
      (metadata.ansible,','.join(self.conf.rbs))
    subprocess.check_call(['bash','-c',command_string])

    #start routing service on all brokers
    command_string='cd %s && ansible-playbook playbooks/experiment/rs.yml  --limit %s'%\
      (metadata.ansible,self.conf.brokers)
    subprocess.check_call(['bash','-c',command_string])

    #start EdgeBroker on ebs
    command_string='cd %s && ansible-playbook playbooks/experiment/eb.yml  --limit %s\
      --extra-vars="zk_connector=%s emulated_broker=%d"'%\
      (metadata.ansible,','.join(self.conf.ebs),metadata.zk,1)
    subprocess.check_call(['bash','-c',command_string])

    #start RoutingBroker on rbs
    command_string='cd %s && ansible-playbook playbooks/experiment/rb.yml  --limit %s\
      --extra-vars="zk_connector=%s emulated_broker=%d"'%(metadata.ansible,','.join(self.conf.rbs),metadata.zk,1)
    subprocess.check_call(['bash','-c',command_string])

   

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for setting up  pubsubcoord infrastructure nodes')
  parser.add_argument('conf',help='configuration file containing setup information')
  parser.add_argument('--teardown',dest='teardown',action='store_true',help='flag to specify if infrastructure processes must be restarted')
  args=parser.parse_args()
  Infrastructure(args.conf,args.teardown).clean()
