from kazoo.client import KazooClient
import argparse,metadata,conf,json,subprocess

class Query(object):
  def __init__(self):
    self._zk=KazooClient(hosts=metadata.zk)
    self._zk.start()
    self.conf=conf.Conf('conf/conf.csv')
  
  def children(self,path):
    num=self._zk.get_children(path)
    joined_sub=set(num)
    all_clients=set(self.conf.publishers.keys())
    
    remaining=all_clients-(joined_sub & all_clients)
    print(remaining)
    return remaining

  def launch_remaining(self,path):
    remaining=self.children(path)
    pairs={k:self.conf.subscribers[k] for k in remaining} 
    rep=json.dumps(pairs)
    subprocess.check_call(['python','src/start_subscribers.py',rep,str(self.conf.sub_sample_count),self.conf.run_id])

  def close(self):
    self._zk.close()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script to query zk tree')
  parser.add_argument('action',help='action to perform children')
  parser.add_argument('path',help='zk path for the query')
  args=parser.parse_args()
  
  if args.action=='children':
    Query().children(args.path)
  elif args.action=='launch':
    Query().launch_remaining(args.path)
