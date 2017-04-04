import metadata,argparse,experiment3,infrastructure

def create_conf(num_pub):
  num_clients=num_pub//metadata.max_publishers_per_host
  clients=['cli1-%d'%(i+2) for i in range(num_clients)]
  pub_distribution=['%s:t1:%d'%(cli,metadata.max_publishers_per_host) for cli in clients ]
  #add client machine for subscriber to clients
  clients.append('cli1')
  pub_sample_count=1000
  conf="""rbs:rb1
ebs:eb1
clients:%s
topics:t1
no_subs:1
no_pubs:%d
sub_distribution:cli1:t1:1
pub_distribution:%s
pub_sample_count:%d
sub_sample_count:%d
sleep_interval:10"""%(','.join(clients),num_pub,\
  ','.join(pub_distribution),pub_sample_count,num_pub*pub_sample_count)
  print(conf)
  with open('conf/conf.csv','w') as f:
    f.write(conf)

def run(min_pub,step_size,max_pub,kill):
  for num in range(min_pub,\
    max_pub+metadata.max_publishers_per_host,\
    step_size):
      create_conf(num)
      infrastructure.Infrastructure('conf/conf.csv',True).setup()
      experiment3.Experiment('conf/conf.csv','%d'%(num),kill).run()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting latency vs #pub stress test')
  parser.add_argument('min_pub',type=int,help='minimum number of publishers to start from (multiple of max_publishers_per_host)')
  parser.add_argument('step_size',type=int,help='step size with which to iterate from min_sub to max_sub (multiple of max_publishers_per_host)')
  parser.add_argument('max_pub',type=int,help='maximum number of subscribers (multiple of max_publishers_per_host)')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to determine whether to kill pre-existing client and monitoirng processes.')
  args=parser.parse_args()

  run(args.min_pub,args.step_size,args.max_pub,args.kill)
