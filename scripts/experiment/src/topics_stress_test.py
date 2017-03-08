import metadata,argparse,experiment3,os,infrastructure

def create_conf(num_topics):
  max_endpoints_per_host=10
  topics=['t%d'%(i+1) for i in range(num_topics)]
  num_clients=num_topics//max_endpoints_per_host
  remaining=num_topics%max_endpoints_per_host
  if num_clients==0:
    clients=['cli1-2']
    sub_distribution=['cli1-2:t%d:1'%(i+1) for i in range(remaining)]
  else:
    clients=['cli1-%d'%(i+2) for i in range(num_clients)]
    sub_distribution=['cli1-%d:t%d:1'%(cli+2,max_endpoints_per_host*cli+i+1) \
      for i in range(max_endpoints_per_host)\
      for cli in range(num_clients)]
    if remaining>0:
      client='cli1-%d'%(num_clients+2)
      clients.append(client)
      for i in range(remaining):
        sub_distribution.append('%s:t%d:1'%(client,num_clients*max_endpoints_per_host+i+1))

  conf="""rbs:rb1
ebs:eb1
clients:%s
topics:%s
no_subs:%d
no_pubs:%d
sub_distribution:%s
pub_distribution:%s
pub_sample_count:5000
sub_sample_count:5000
sleep_interval:50"""%(','.join(clients),','.join(topics),\
  num_topics,num_topics,','.join(sub_distribution),','.join(sub_distribution))
  print(conf)
  with open('conf/conf.csv','w') as f:
    f.write(conf)

def run(min_topics,step_size,max_topics,kill):
  for num in range(min_topics,max_topics+step_size,step_size):
      create_conf(num)
      infrastructure.Infrastructure('conf/conf.csv',True).setup()
      experiment3.Experiment('conf/conf.csv','%d'%(num),kill).run()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting #topics stress test')
  parser.add_argument('min_topics',type=int,help='minimum number of topics to start from')
  parser.add_argument('step_size',type=int,help='step size with with to iterate from min_topics to max_topics')
  parser.add_argument('max_topics',type=int,help='maximum number of topics')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to determine whether to kill pre-existing client and monitoirng processes.')
  args=parser.parse_args()

  run(args.min_topics,args.step_size,args.max_topics,args.kill)
