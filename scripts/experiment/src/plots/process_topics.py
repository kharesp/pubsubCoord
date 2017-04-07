import numpy as np
import os,metadata
import matplotlib.pyplot as plt

def _get_topic_files_map(test_dir,num_topics):
  topic_files_map={}
  for f in os.listdir(test_dir):
    if(os.path.isfile(os.path.join(test_dir,f)) and f.startswith('t')):
      topic=f.partition('_')[0]
      if topic in topic_files_map:
        topic_files_map[topic].append(test_dir+'/'+f)
      else:
        topic_files_map[topic]=[test_dir+'/'+f]
  return topic_files_map

def process_topic_files(test_dir,num_topics):
  topic_files_map=_get_topic_files_map(test_dir,num_topics)
  with open('%s/summary_topic.csv'%(test_dir),'w') as out:
    out.write(metadata.latency_summary_header)
    for topic,files in sorted(topic_files_map.items()):
      data=[np.genfromtxt(f,dtype='int,int',delimiter=',',\
        usecols=[4,5],skip_header=1)[metadata.initial_samples:] for f in files]

      lengths=[len(arr) for arr in data]
      min_len=min(lengths)

      curated_data=[arr[:min_len] for arr in data]

      arr=np.array(curated_data,dtype=np.dtype('int,int'))
      latencies=arr['f0']
      interarrival=arr['f1']
      min_interarrival=min([min(arr[np.nonzero(arr)]) for arr in interarrival])
     
      out.write('%s,%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n'%\
        (topic,len(files),\
        np.mean(latencies),np.std(latencies),\
        np.amin([arr[np.nonzero(arr)] for arr in latencies]),\
        np.amax(latencies),\
        np.percentile(latencies,90),np.percentile(latencies,99),\
        np.percentile(latencies,99.9),np.percentile(latencies,99.99),\
        np.percentile(latencies,99.999),np.percentile(latencies,99.9999),\
        np.mean(interarrival),np.std(interarrival),\
        min_interarrival,\
        np.amax(interarrival),\
        np.percentile(interarrival,90),np.percentile(interarrival,99),\
        np.percentile(interarrival,99.9),np.percentile(interarrival,99.99),\
        np.percentile(interarrival,99.999),np.percentile(interarrival,99.9999)))

def plot_latency_per_topic(test_dir,num_topics):
  topic_files_map=_get_topic_files_map(test_dir,num_topics)
  legend=[]
  for topic,files in sorted(topic_files_map.items()):
    legend.append('y=%s'%(topic))
    latencies=[np.genfromtxt(f,dtype=None,delimiter=',',\
      usecols=[4],skip_header=1)[metadata.initial_samples:] for f in files]
    lengths=[len(arr) for arr in latencies]
    min_len=min(lengths)
    curated=[l[:min_len] for l in latencies]
    res=np.sum(curated,axis=0)/(1.0*len(files))
    plt.plot(res)

  plt.legend(legend)
  plt.savefig('%s/latency.png'%(test_dir))
  plt.close()

def plot_per_subscriber_latency(test_dir):
  for i in os.listdir(test_dir):
    curr_file='%s/%s'%(test_dir,i)
    if (os.path.isfile(curr_file) and i.startswith('t')):
      latency=np.genfromtxt(curr_file,dtype=None,delimiter=',',\
        usecols=[4],skip_header=1)[metadata.initial_samples:]
      
      plt.plot(latency)
      plt.savefig('%s/lplot_%s.png'%(test_dir,i.rpartition('.')[0]))
      plt.close()

if __name__=="__main__":
  print(_get_topic_files_map('/home/kharesp/workspace/ansible/pubsubCoord/logs/5',5))
