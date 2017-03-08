import argparse
import numpy as np 
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

def plot(log_dir,topic_count_list):
  with open('%s/topic_stress_test_latency.csv'%(log_dir),'w') as f:
    for i in sorted(topic_count_list):
      latencies=np.genfromtxt('%s/%s/summary_topic.csv'%(log_dir,i),delimiter=',',\
        usecols=2,skip_header=1)
      f.write('%d,%f\n'%(len(latencies),np.mean(latencies)))
      
  with open('%s/topic_stress_test_eb.csv'%(log_dir),'w') as f:
    for i in sorted(topic_count_list):
      lines=open('%s/%s/summary_routing_service_eb.csv'%(log_dir,i),'r').readlines()
      f.write(lines[1])

  data=np.genfromtxt('%s/topic_stress_test_latency.csv'%(log_dir),dtype='int,float',delimiter=',',usecols=[0,1])
  topics=data['f0']
  mean_latency=data['f1']

  utilization=np.genfromtxt('%s/topic_stress_test_eb.csv'%(log_dir),dtype='float,float,float,float,float,float',delimiter=',',usecols=[1,2,5,6,9,10])
  mean_cpu=utilization['f0']
  std_cpu=utilization['f1']
  mean_host_cpu=utilization['f2']
  std_host_cpu=utilization['f3']
  mean_mem_mb=utilization['f4']/1000
  std_mem_mb=utilization['f5']/1000

  gs=gridspec.GridSpec(2,2)

  #subplot for latency vs topics 
  latency=plt.subplot(gs[0,:])
  latency.plot(topics, mean_latency, marker='o', label='average latency (ms)')
  latency.set_ylabel('average latency (ms)')
  latency.set_xticks(topics)
  latency.yaxis.grid(True)
  #annotate data points
  for i, val in enumerate(mean_latency):
    latency.annotate('%.1f'%(val),(topics[i],mean_latency[i]),verticalalignment='bottom',horizontalalignment='top',size=11)
  latency.legend()


  #subplot for cpu/mem utilization vs topics
  util=plt.subplot(gs[1,:])
  #cpu=util.plot(topics,mean_cpu,marker='s',color='b',label='cpu utilization (%)')
  host_cpu=util.plot(topics,mean_host_cpu,marker='s',color='b',label='host cpu utilization (%)')
  util.set_xticks(topics)
  util.set_xlabel('number of topics')
  util.set_ylabel('cpu utilization (%)')
  util.yaxis.grid(True)

  #add another scale for plotting memory utilization
  ax2=util.twinx()
  mem=ax2.plot(topics,mean_mem_mb,marker='^',color='g',label='memory (mb)')
  ax2.set_ylabel('memory utilization (mb)')

  #create unified legend for both scales
  lns=host_cpu+mem
  labels=[l.get_label() for l in lns]
  util.legend(lns,labels,loc=0)

  #save created plot
  plt.tight_layout()
  plt.savefig('%s/topic_stress_test_plot.pdf'%(log_dir),bbox_inches='tight')
  plt.close()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for plotting results for topics stress test')
  parser.add_argument('topic_count_list',help='list of topic counts for which to plot results for')
  parser.add_argument('log_dir',help='log directory path')
  args=parser.parse_args()

  plot(args.log_dir,[int(i) for i in args.topic_count_list.split(',')])
