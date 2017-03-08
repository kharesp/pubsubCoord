class Conf(object):
  def __init__(self,conf_file):
    self.conf_file=conf_file
    self.parse()
    
  def parse(self):
    with open(self.conf_file) as f:
      for line in f:
        if line.startswith('run_id'):
          self.run_id=line.rstrip().partition(':')[2]
        elif line.startswith('rbs'):
          self.rbs=line.rstrip().partition(':')[2].split(',')
        elif line.startswith('ebs'):
          self.ebs=line.rstrip().partition(':')[2].split(',')
        elif line.startswith('clients'):
          self.clients=line.rstrip().partition(':')[2].split(',')
        elif line.startswith('topics'):
          self.topics=line.rstrip().partition(':')[2].split(',')
          self.no_topics=len(self.topics)
        elif line.startswith('no_subs'):
          self.no_subscribers=int(line.rstrip().partition(':')[2])
        elif line.startswith('no_pubs'):
          self.no_publishers=int(line.rstrip().partition(':')[2])
        elif line.startswith('sub_distribution'):
          self.subscribers={}
          for sub_description in line.rstrip().partition(':')[2].split(','):
            host,topic,num_sub= sub_description.split(':')
            if host in self.subscribers:
              self.subscribers[host].update({topic: num_sub})
            else:
              self.subscribers[host]={topic: num_sub}
        elif line.startswith('pub_distribution'):
          self.publishers={}  
          for pub_description in line.rstrip().partition(':')[2].split(','):
            host,topic,num_pub= pub_description.split(':')
            if host in self.publishers:
              self.publishers[host].update({topic: num_pub})
            else:
              self.publishers[host]={topic: num_pub}
        elif line.startswith('pub_sample_count'):
          self.pub_sample_count=int(line.rstrip().partition(':')[2])
        elif line.startswith('sub_sample_count'):
          self.sub_sample_count=int(line.rstrip().partition(':')[2])
        elif line.startswith('sleep_interval'):
          self.sleep_interval=int(line.rstrip().partition(':')[2])
        else:
          print('invalid line:%s'%(line))
    self.brokers= ','.join(self.ebs)+','+','.join(self.rbs)
    self.hosts=self.brokers+','+','.join(self.clients)
    self.client_numSubscribers={ client: sum([int(num_sub) for num_sub in topics.values()]) for client,topics in self.subscribers.items() }
    self.client_numPublishers={ client: sum([int(num_pub) for num_pub in topics.values()]) for client,topics in self.publishers.items() }

if __name__=="__main__":
  Conf('conf/conf.csv')
