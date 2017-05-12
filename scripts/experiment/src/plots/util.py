import numpy as np
file_name="/home/kharesp/workspace/ansible/pubsubCoord/logs/rate_5ms_run_1/400/util_eb1_25793.csv"
data=np.genfromtxt(file_name,dtype='float,float',delimiter=';',\
        usecols=[4,12],skip_header=1)
print(np.mean(data['f0']))
print(np.mean(data['f1']))
