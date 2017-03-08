import argparse
import numpy as np 
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

def plot(log_dir,subscribers):
  with open('%s/subscriber_stress_test_latency.csv'%(log_dir),'w') as f:
    for i in sorted(subscribers):
      lines=open('%s/%s/summary_topic.csv'%(log_dir,i),'r').readlines()
      f.write(lines[1])
  with open('%s/subscriber_stress_test_eb.csv'%(log_dir),'w') as f:
    for i in sorted(subscribers):
      lines=open('%s/%s/summary_routing_service_eb.csv'%(log_dir,i),'r').readlines()
      f.write(lines[1])

  data=np.genfromtxt('%s/subscriber_stress_test_latency.csv'%(log_dir),dtype='int,float,float,float,float',delimiter=',',usecols=[1,2,3,7,8])
  subscribers=data['f0']
  mean_latency=data['f1']
  std_latency=data['f2']
  per_99_latency=data['f3']
  per_99_9_latency=data['f4']

  utilization=np.genfromtxt('%s/subscriber_stress_test_eb.csv'%(log_dir),dtype='float,float,float,float,float,float',delimiter=',',usecols=[1,2,5,6,9,10])
  mean_cpu=utilization['f0']
  std_cpu=utilization['f1']
  mean_host_cpu=utilization['f2']
  std_host_cpu=utilization['f3']
  mean_mem_mb=utilization['f4']/1000
  std_mem_mb=utilization['f5']/1000

  gs=gridspec.GridSpec(2,2)

  #subplot for latency vs subscribers
  latency=plt.subplot(gs[0,:])
  #latency.errorbar(subscribers, mean_latency, yerr=std_latency, capsize=2, marker='o', label='mean')
  latency.plot(subscribers, mean_latency, marker='o', label='average latency (ms)')
  #latency.plot(subscribers,per_99_latency, marker='o', color='y', label='99th percentile')
  #latency.plot(subscribers,per_99_9_latency, marker='o', color='r', label='99.9 percentile')
  latency.set_ylabel('latency (ms)')
  #latency.set_xlabel('number of subscribers')
  latency.set_xticks(subscribers)
  latency.yaxis.grid(True)
  #annotate data points
  for i, val in enumerate(mean_latency):
    latency.annotate('%.1f'%(val),(subscribers[i],mean_latency[i]),verticalalignment='bottom',horizontalalignment='top',size=11)
  latency.legend()


  #subplot for cpu/mem utilization vs subscribers
  util=plt.subplot(gs[1,:])
  #cpu=util.plot(subscribers,mean_cpu,marker='s',color='b',label='cpu utilization (%)')
  host_cpu=util.plot(subscribers,mean_host_cpu,marker='s',color='b',label='host cpu utilization (%)')
  util.set_xticks(subscribers)
  util.set_xlabel('number of subscribers')
  util.set_ylabel('cpu utilization (%)')
  util.yaxis.grid(True)

  #add another scale for plotting memory utilization
  ax2=util.twinx()
  mem=ax2.plot(subscribers,mean_mem_mb,marker='^',color='g',label='memory (mb)')
  ax2.set_ylabel('memory utilization (mb)')

  #create unified legend for both scales
  lns=host_cpu+mem
  labels=[l.get_label() for l in lns]
  util.legend(lns,labels,loc=0)

  #save created plot
  plt.tight_layout()
  plt.savefig('%s/subscriber_stress_test_plot.pdf'%(log_dir),bbox_inches='tight')
  plt.close()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for plotting results for subscriber stress test')
  parser.add_argument('subscriber_count_list',help='list of subscriber counts for which to plot results for')
  parser.add_argument('log_dir',help='log directory path')
  args=parser.parse_args()

  plot(args.log_dir,[int(i) for i in args.subscriber_count_list.split(',')])
