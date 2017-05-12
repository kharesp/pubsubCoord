import numpy as np

rates= [5,10,20,40]
runs=[1,2,3]
parent_dir="/home/kharesp/workspace/ansible/pubsubCoord/logs/poisson"

for rate in rates:
  print("\n\nrate:%d"%(rate))
  rate_dir="%s/rate_%dms"%(parent_dir,rate)
  for run in runs:
    print("\nrun:%d"%(run))
    run_dir="%s/rate_%dms_run_%d"%(rate_dir,rate,run)
    for i in range(80,480,80):
      file_name="%s/%d/summary_topic.csv"%(run_dir,i)
      data=np.genfromtxt(file_name,dtype='float',delimiter=',',\
            usecols=[2],skip_header=1)
      print(data)
