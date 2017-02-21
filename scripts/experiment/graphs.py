import os,sys,datetime,time
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as md
import matplotlib.ticker as ticker

#plot cpu/time and memory/time plot for rbs
def broker_cpumem_vs_time(test_dir,broker_type):
  broker_files=[]
  #collect relevant broker files
  for i in os.listdir(test_dir):
    if (os.path.isfile(os.path.join(test_dir,i)) and \
      i.startswith('routing_service_%s_'%(broker_type))):
      broker_files.append(test_dir+"/"+i)

  num_rbs=len(broker_files)
  fig=plt.figure()

  for i in range(num_rbs):
    #datetime format for x axis
    xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
    broker='%s_%s'%(broker_type,broker_files[i].rpartition('/')[2].split('_')[3])
    arr=np.genfromtxt(broker_files[i],dtype=None,delimiter=',',usecols=[0,4,6])
    dates=[datetime.datetime.fromtimestamp(time.mktime(time.gmtime(ts/1000.0))) for \
      ts in arr['f0']]
    datenums=md.date2num(dates)

    rb_cpu_vs_time=fig.add_subplot(num_rbs,2,2*i+1)
    rb_cpu_vs_time.xaxis.set_major_formatter(xfmt)
    plt.xticks(rotation=20)
    rb_cpu_vs_time.xaxis.set_major_locator(md.SecondLocator(interval=120))
    rb_cpu_vs_time.grid(True)
    rb_cpu_vs_time.set_ylabel('%cpu usage')
    rb_cpu_vs_time.set_xlabel('time')
    rb_cpu_vs_time.set_title('%s cpu utilization'%(broker))
    rb_cpu_vs_time.plot(datenums,arr['f1'],'-r')

    rb_mem_vs_time=fig.add_subplot(num_rbs,2,2*i+2)
    rb_mem_vs_time.xaxis.set_major_formatter(xfmt)
    plt.xticks(rotation=20)
    rb_mem_vs_time.xaxis.set_major_locator(md.SecondLocator(interval=120))
    rb_mem_vs_time.grid(True)
    rb_mem_vs_time.set_ylabel('memory usage(Mb)')
    rb_mem_vs_time.set_xlabel('time')
    rb_mem_vs_time.set_title('%s memory utilization'%(broker))
    rb_mem_vs_time.plot(datenums,np.round(arr['f2']/1000,decimals=2),'-g')

  fig.subplots_adjust(wspace=.1,hspace=.1)
  fig.tight_layout()
  #plt.suptitle('broker cpu and memory utilization')
  plt.savefig('%s/%s_cpumem.png'%(test_dir,broker_type))
  plt.close()

def get_topic_files_map(test_dir,num_topics):
  topic_files_map={}
  for t in range(num_topics):
    for f in os.listdir(test_dir):
      if(os.path.isfile(os.path.join(test_dir,f)) and f.startswith('t')):
        topic=f.partition('_')[0]
        if topic in topic_files_map:
          topic_files_map[topic].append(test_dir+'/'+f)
        else:
          topic_files_map[topic]=[test_dir+'/'+f]

  return topic_files_map

def per_sample_latency(test_dir,num_topics):
  topic_files_map=get_topic_files_map(test_dir,num_topics)
  legend=[]
  for topic,files in sorted(topic_files_map.items()):
    legend.append('y=%s'%(topic))
    latencies=[np.genfromtxt(f,dtype=None,delimiter=',',usecols=[4]) for f\
      in files]
    res=np.sum(latencies,axis=0)/(1.0*len(files))
    plt.plot(res[500:])

  plt.legend(legend)
  plt.savefig('%s/latency.png'%(test_dir))
  plt.close()

def summary_statistics(test_dir,num_topics):
  topic_files_map=get_topic_files_map(test_dir,num_topics)
  with open('%s/summary.csv'%(test_dir),'w') as out:
    out.write('topic,#subscribers,mean latency(ms),min latency(ms),max latency(ms),\
90%ile latency(ms),99%ile latency(ms),99.9%ile latency(ms),\
99.99%ile latency(ms),99.999%ile latency(ms),99.9999%ile latency(ms)\n')
    for topic,files in sorted(topic_files_map.items()):
      latencies=[np.genfromtxt(f,dtype=None,delimiter=',',usecols=[4])[500:] for f\
        in files]
      out.write('%s,%d,%f,%f,%f,%f,%f,%f,%f,%f,%f\n'%\
        (topic,len(files),np.mean(latencies),np.amin(latencies),np.amax(latencies),\
        np.percentile(latencies,90),np.percentile(latencies,99),\
        np.percentile(latencies,99.9),np.percentile(latencies,99.99),\
        np.percentile(latencies,99.999),np.percentile(latencies,99.9999)))

if __name__=="__main__":
  per_sample_latency('/home/kharesp/workspace/ansible/pubsubCoord/logs/1',1)
