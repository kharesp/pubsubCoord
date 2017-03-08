import numpy as np
import argparse
import matplotlib.pyplot as plt

def summarize(log_dir,topic_list,locality_list):
  with open('%s/summary_locality_test.csv'%(log_dir),'w') as f: 
    f.write('topics,locality,avg_latency(ms),avg_eb_host_cpu(%),avg_eb_host_mem(mb),#rb,avg_rb_host_cpu(%),avg_rb_host_mem(mb)\n')
    for topic in sorted(topic_list):
      for locality in sorted(locality_list):
        latencies=np.genfromtxt('%s/%d/locality_%s/summary_topic.csv'%(log_dir,topic,locality),\
          delimiter=',', usecols=2,skip_header=1,dtype='float')
        avg_latency=np.mean(latencies)

        eb_utilization=np.genfromtxt('%s/%d/locality_%s/summary_routing_service_eb.csv'%(log_dir,topic,locality),\
          delimiter=',', usecols=[5,9],skip_header=1,dtype='float,float')
        avg_host_cpu_eb=np.mean(eb_utilization['f0'])
        avg_host_mem_mb_eb=np.mean(eb_utilization['f1']/1000)

        rb_utilization=np.genfromtxt('%s/%d/locality_%s/summary_routing_service_rb.csv'%(log_dir,topic,locality),\
          delimiter=',', usecols=[5,9],skip_header=1,dtype='float,float')
        num_rb=rb_utilization.size
        avg_host_cpu_rb=np.mean(rb_utilization['f0'])
        avg_host_mem_mb_rb=np.mean(rb_utilization['f1']/1000)
        f.write('%d,%s,%f,%f,%f,%d,%f,%f\n'%(topic,locality,avg_latency,\
          avg_host_cpu_eb,avg_host_mem_mb_eb,num_rb,avg_host_cpu_rb,avg_host_mem_mb_rb))

def plot(log_dir,topic_list,locality_list):
  data=np.genfromtxt('%s/summary_locality_test.csv'%(log_dir), delimiter=',',\
    dtype='int,float,float,float,float,int,float,float',skip_header=1)

  localities_data={}
  for locality in locality_list:
    localities_data[locality]=data[data['f1']==float(locality)]

  #plot latency vs #topics for different localities
  fig,ax= plt.subplots() 
  ax.set_xlabel('#topics at each EB')
  ax.set_xticks(topic_list)
  ax.set_ylabel('latency (ms)')

  markers=['*','d','s'] 
  for idx,locality in enumerate(locality_list):
    ax.plot(topic_list,localities_data[locality]['f2'],\
      label='locality=%s'%(locality),marker=markers[idx])

  ax.legend()
  plt.savefig('%s/locality_test_latency.pdf'%(log_dir))
  plt.close()

  #plot EB cpu/memory utilization vs #topics for different localities
  eb_cpu=plt.subplot(2,1,1)
  eb_cpu.set_xticks(topic_list)
  eb_cpu.set_ylabel('EB host cpu utilization (%)')
  eb_cpu.yaxis.grid(True)
  for idx,locality in enumerate(locality_list):
    eb_cpu.plot(topic_list,localities_data[locality]['f3'],\
      label='locality=%s'%(locality),marker=markers[idx])

  eb_cpu.legend()
  
  eb_mem=plt.subplot(2,1,2)
  eb_mem.set_xlabel('#topics at each EB')
  eb_mem.set_xticks(topic_list)
  eb_mem.set_ylabel('EB host memory utilization (mb)')
  eb_mem.yaxis.grid(True)
  for idx,locality in enumerate(locality_list):
    eb_mem.plot(topic_list,localities_data[locality]['f4'],\
      label='locality=%s'%(locality),marker=markers[idx])
  eb_mem.legend()
  
  plt.tight_layout()
  plt.savefig('%s/locality_test_eb.pdf'%(log_dir),bbox_inches='tight')
  plt.close()

  #plot RB cpu/memory utilization vs #topics for different localities
  rb_cpu=plt.subplot(2,1,1)
  rb_cpu.set_xticks(topic_list)
  rb_cpu.set_ylabel('RB host cpu utilization (%)')
  rb_cpu.yaxis.grid(True)
  for idx,locality in enumerate(locality_list):
    rb_cpu.plot(topic_list,localities_data[locality]['f6'],\
      label='locality=%s'%(locality),marker=markers[idx])

  rb_cpu.legend()
  
  rb_mem=plt.subplot(2,1,2)
  rb_mem.set_xlabel('#topics at each EB')
  rb_mem.set_xticks(topic_list)
  rb_mem.set_ylabel('RB host memory utilization (mb)')
  rb_mem.yaxis.grid(True)
  for idx,locality in enumerate(locality_list):
    rb_mem.plot(topic_list,localities_data[locality]['f7'],\
      label='locality=%s'%(locality),marker=markers[idx])
  rb_mem.legend()
  
  plt.tight_layout()
  plt.savefig('%s/locality_test_rb.pdf'%(log_dir),bbox_inches='tight')
  plt.close()

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for summarizing results for locality test')
  parser.add_argument('log_dir',help='log directory path')
  parser.add_argument('topic_count_list',help='list of topic counts for which to summarize results')
  parser.add_argument('locality_list',help='list of data localities for which to summarize results')
  args=parser.parse_args()

  summarize(args.log_dir,[int(i) for i in args.topic_count_list.split(',')],\
    [i for i in args.locality_list.split(',')])
  plot(args.log_dir,[int(i) for i in args.topic_count_list.split(',')],\
    [i for i in args.locality_list.split(',')])
