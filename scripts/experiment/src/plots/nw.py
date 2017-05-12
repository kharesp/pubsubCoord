import numpy as np
file_name="/home/kharesp/workspace/ansible/pubsubCoord/logs/rate_40ms_run_1/400/nw_eb1_12414.csv"
data=np.genfromtxt(file_name,dtype='float,float',delimiter=';',\
        usecols=[4,5],skip_header=1)
print(np.mean(data['f0']))
print(np.mean(data['f1']))
