from kazoo.client import KazooClient
from kazoo.recipe.barrier import Barrier
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType
import metadata,conf

class Zk(object):
  def __init__(self,run_id,conf):
    self.run_id=run_id
    self.conf=conf
    self._zk=KazooClient(hosts=metadata.zk)
    self._zk.start()

  def clean(self):
   #delete zk /topics path
   if self._zk.exists(metadata.topics_path):
     self._zk.delete(metadata.topics_path,recursive=True)

   #delete zk /experiment path
   if self._zk.exists(metadata.experiment_path):
     self._zk.delete(metadata.experiment_path,recursive=True)

  def stop(self):
    self._zk.stop()

  def setup(self):
    self.create_paths() 
    self.install_watches()

  def wait(self,barrier):
    if (barrier=='subscriber'):
      self.sub_barrier.wait()
    elif(barrier=='finished'):
      self.finished_barrier.wait()
    elif(barrier=='monitoring'):
      self.monitoring_barrier.wait()
    else:
      print('invalid barrier name')

  def create_paths(self):
    #create zk path for subscribers
    for client in self.conf.client_numSubscribers.keys():
      sub_path='%s/%s/sub/region_%s/%s'%\
        (metadata.experiment_path,self.run_id,client[3:client.index('-')],client)
      self._zk.ensure_path(sub_path)
    
    #create zk path for publishers
    for client in self.conf.client_numPublishers.keys():
      pub_path='%s/%s/pub/region_%s/%s'%\
        (metadata.experiment_path,self.run_id,client[3:client.index('-')],client)
      self._zk.ensure_path(pub_path)

    #create zk path to track joined subscribers and publishers
    joined_sub_path='%s/%s/joined_sub'%\
      (metadata.experiment_path,self.run_id)
    self._zk.ensure_path(joined_sub_path)
    joined_pub_path='%s/%s/joined_pub'%\
      (metadata.experiment_path,self.run_id)
    self._zk.ensure_path(joined_pub_path)

    #create zk path to track monitoring processes have joined
    eb_monitoring_path='%s/%s/monitoring/eb'%(metadata.experiment_path,self.run_id)
    self._zk.ensure_path(eb_monitoring_path)
    self.eb_monitors_exited=False

    rb_monitoring_path='%s/%s/monitoring/rb'%(metadata.experiment_path,self.run_id)
    self._zk.ensure_path(rb_monitoring_path)
    self.rb_monitors_exited=False
   
    #create barrier paths 
    sub_barrier_path='%s/%s/barriers/sub'%(metadata.experiment_path,self.run_id)
    self._zk.ensure_path(sub_barrier_path)
    pub_barrier_path='%s/%s/barriers/pub'%(metadata.experiment_path,self.run_id)
    self._zk.ensure_path(pub_barrier_path)
    finished_barrier_path='%s/%s/barriers/finished'%(metadata.experiment_path,self.run_id)
    self._zk.ensure_path(finished_barrier_path)
    monitoring_barrier_path='%s/%s/barriers/monitoring'%(metadata.experiment_path,self.run_id)
    self._zk.ensure_path(monitoring_barrier_path)

    #create barriers
    self.sub_barrier=Barrier(client=self._zk,path=sub_barrier_path)
    self.finished_barrier=Barrier(client=self._zk,path=finished_barrier_path)
    self.pub_barrier=Barrier(client=self._zk,path=pub_barrier_path)
    self.monitoring_barrier=Barrier(client=self._zk,path=monitoring_barrier_path)

  def install_watches(self):
    region_joined_subscriber_clients={region:0 for region in \
      self.conf.region_clientsubscribers_map.keys()}

    region_joined_publisher_clients={region:0 for region in \
      self.conf.region_clientpublishers_map.keys()}

    #listener callback to track joined publishers and subscribers in all regions
    def _joined_endpoint_listener(children,event):
      if event and event.type==EventType.CHILD:
        if 'sub' in event.path :
          client=event.path.rpartition('/')[2]
          region=client[3:client.index('-')]
          if (len(children)==self.conf.client_numSubscribers[client]):
            print('All subscribers have joined on client:%s\n'%(client))
            region_joined_subscriber_clients[region]+=1
            if (region_joined_subscriber_clients[region]==\
              self.conf.region_clientsubscribers_map[region]):
              self._zk.ensure_path('%s/%s/joined_sub/region_%s'%\
                (metadata.experiment_path,self.run_id,region))
          if (len(children)==0):
            print('All subscribers on client:%s have exited\n'%(client))
            region_joined_subscriber_clients[region]-=1
            if (region_joined_subscriber_clients[region]==0):
              self._zk.delete('%s/%s/joined_sub/region_%s'\
                %(metadata.experiment_path,self.run_id,region))
            return False
        if 'pub' in event.path: 
          client=event.path.rpartition('/')[2]
          region=client[3:client.index('-')]
          if (len(children)==self.conf.client_numPublishers[client]):
            print('All publishers have joined on client:%s\n'%(client))
            region_joined_publisher_clients[region]+=1
            if (region_joined_publisher_clients[region]==\
              self.conf.region_clientpublishers_map[region]):
              self._zk.ensure_path('%s/%s/joined_pub/region_%s'%\
                (metadata.experiment_path,self.run_id,region))
            return False
          
    #listener callback to open zk barrier: sub,pub, finished or monitoring
    def _open_barrier(children,event):
      if event and event.type==EventType.CHILD:
        if 'joined_sub' in event.path:
          if (len(children)==len(self.conf.region_clientsubscribers_map)):
            print("All subscribers have joined. Opening subscriber barrier\n")
            self.sub_barrier.remove()
          if (len(children)==0):
            print("All subscribers have left. Opening finished barrier\n")
            self.finished_barrier.remove()
            return False
        if 'joined_pub' in event.path:
          if (len(children)==len(self.conf.region_clientpublishers_map)):
            print("All publishers have joined. Opening publisher barrier\n")
            self.pub_barrier.remove()
            return False
        if 'monitoring/eb' in event.path:
          if (len(children)==0):
            print('All eb monitors have exited')
            self.eb_monitors_exited=True
            if (self.eb_monitors_exited and self.rb_monitors_exited):
              print('All monitors have exited. Opening monitoring barrier')
              self.monitoring_barrier.remove()
            return False
        if 'monitoring/rb' in event.path:
          if (len(children)==0):
            print('All rb monitors have exited')
            self.rb_monitors_exited=True
            if (self.eb_monitors_exited and self.rb_monitors_exited):
              print('All monitors have exited. Opening monitoring barrier')
              self.monitoring_barrier.remove()
            return False

    #watch to open subscriber barrier once all subscribers in all regions have joined
    sub_barrier_opener_watch=ChildrenWatch(client=self._zk,\
      path='%s/%s/joined_sub'%(metadata.experiment_path,self.run_id),\
      func=_open_barrier,send_event=True)

    #watch to open publisher barrier once all publishers in all regions have joined
    pub_barrier_opener_watch=ChildrenWatch(client=self._zk,\
      path='%s/%s/joined_pub'%(metadata.experiment_path,self.run_id),\
      func=_open_barrier,send_event=True)

    #watch to open monitoring barrier when all Monitors have exited
    eb_monitoring_watch=ChildrenWatch(client=self._zk,\
      path='%s/%s/monitoring/eb'%(metadata.experiment_path,self.run_id),\
      func=_open_barrier,send_event=True)

    rb_monitoring_watch=ChildrenWatch(client=self._zk,\
      path='%s/%s/monitoring/rb'%(metadata.experiment_path,self.run_id),\
      func=_open_barrier,send_event=True)

    #watches for tracking joined subscribers
    joined_sub_watches=[ChildrenWatch(client=self._zk,\
      path='%s/%s/sub/region_%s/%s'%\
        (metadata.experiment_path,self.run_id,client[3:client.index('-')],client),\
      func=_joined_endpoint_listener,send_event=True)\
      for client in self.conf.client_numSubscribers.keys()]

    #watches for tracking joined publishers
    joined_pub_watches=[ChildrenWatch(client=self._zk,\
      path='%s/%s/pub/region_%s/%s'%\
       (metadata.experiment_path,self.run_id,client[3:client.index('-')],client),\
      func=_joined_endpoint_listener,send_event=True)\
      for client in self.conf.client_numPublishers.keys()]
