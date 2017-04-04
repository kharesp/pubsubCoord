import metadata,argparse,experiment3,infrastructure

def create_conf(num_sub):
  num_clients=num_sub//metadata.max_subscribers_per_host
  clients=['cli1-%d'%(i+2) for i in range(num_clients)]
  sub_distribution=['%s:t1:%d'%(cli,metadata.max_subscribers_per_host) for cli in clients ]
  #add client machines for publishers to clients
  clients.append('cli1')
  conf="""rbs:rb1
ebs:eb1
clients:%s
topics:t1
no_subs:%d
no_pubs:1
sub_distribution:%s
pub_distribution:cli1:t1:1
pub_sample_count:5000
sub_sample_count:5000
sleep_interval:50"""%(','.join(clients),num_sub,','.join(sub_distribution))
  print(conf)
  with open('conf/conf.csv','w') as f:
    f.write(conf)

def run(min_sub,step_size,max_sub,kill):
  for num in range(min_sub,\
    max_sub+metadata.max_subscribers_per_host,\
    step_size):
      create_conf(num)
      infrastructure.Infrastructure('conf/conf.csv',True).setup()
      experiment3.Experiment('conf/conf.csv','%d'%(num),kill).run()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting latency vs #sub stress test')
  parser.add_argument('min_sub',type=int,help='minimum number of subscribers to start from (multiple of max_subscribers_per_host)')
  parser.add_argument('step_size',type=int,help='step size with which to iterate from min_sub to max_sub (multiple of max_subscribers_per_host)')
  parser.add_argument('max_sub',type=int,help='maximum number of subscribers (multiple of max_subscribers_per_host)')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to determine whether to kill pre-existing client and monitoirng processes.')
  args=parser.parse_args()

  run(args.min_sub,args.step_size,args.max_sub,args.kill)
