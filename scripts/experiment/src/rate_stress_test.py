import metadata,argparse,experiment3,infrastructure

def create_conf(sleep_interval):
  conf="""rbs:rb1
ebs:eb1
clients:cli1,cli1-2
topics:t1
no_subs:1
no_pubs:1
sub_distribution:cli1-2:t1:1
pub_distribution:cli1:t1:1
pub_sample_count:5000
sub_sample_count:5000
sleep_interval:%d"""%(sleep_interval)
  print(conf)
  with open('conf/conf.csv','w') as f:
    f.write(conf)

def run(min_interval,step_size,max_interval,kill):
  for num in range(min_interval,\
    max_interval+step_size,\
    step_size):
      create_conf(num)
      infrastructure.Infrastructure('conf/conf.csv',True).setup()
      experiment3.Experiment('conf/conf.csv','%d'%(num),kill).run()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting latency vs publication rate stress test')
  parser.add_argument('min_interval',type=int,help='minimum sleep interval to start from')
  parser.add_argument('step_size',type=int,help='step size with which to iterate from min_interval to max_interval')
  parser.add_argument('max_interval',type=int,help='maximum sleep interval')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to determine whether to kill pre-existing client and monitoirng processes.')
  args=parser.parse_args()

  run(args.min_interval,args.step_size,args.max_interval,args.kill)
