import os,sys,datetime,time,metadata
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as md
import matplotlib.ticker as ticker

#plot cpu/time and memory/time plot for rbs
def plot_broker_cpumem_vs_time(test_dir,broker_type):
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
    arr=np.genfromtxt(broker_files[i],dtype=None,delimiter=',',usecols=[0,4,6],skip_header=1)
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
  plt.show()
  plt.savefig('%s/%s_cpumem.png'%(test_dir,broker_type))
  plt.close()

def process_routing_service_files(test_dir,broker_type):
  broker_files=[]
  #collect relevant broker files
  for i in os.listdir(test_dir):
    if (os.path.isfile(os.path.join(test_dir,i)) and \
      i.startswith('routing_service_%s_'%(broker_type))):
      broker_files.append(test_dir+"/"+i)

  num_brokers=len(broker_files)
  with open('%s/summary_routing_service_%s.csv'%(test_dir,broker_type),'w') as out:
    out.write(metadata.rs_summary_header)
    for i in range(num_brokers):
      arr=np.genfromtxt(broker_files[i],dtype='float,float,float',delimiter=',',usecols=[4,5,6],skip_header=1)
      broker='%s'%(broker_files[i].rpartition('/')[2].split('_')[3])
      cpu=arr['f0']
      host_cpu=arr['f1']
      mem=arr['f2']
      out.write('%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f\n'%(broker,np.mean(cpu),np.std(cpu),\
        np.amin(cpu),np.amax(cpu),\
        np.mean(host_cpu),np.std(host_cpu),np.amin(host_cpu),np.amax(host_cpu),\
        np.mean(mem),np.std(mem),np.amin(mem),np.amax(mem)))

if __name__=="__main__":
  path='/home/kharesp/workspace/ansible/pubsubCoord/logs'
  for i in os.listdir(path):
    if(os.path.isdir(os.path.join(path,i))):
      process_routing_service_files('%s/%s'%(path,i),'eb')
      process_routing_service_files('%s/%s'%(path,i),'rb')
