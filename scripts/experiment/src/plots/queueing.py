import numpy as np
file_name="/home/kharesp/workspace/ansible/pubsubCoord/logs/rate_10ms_run_2/400/queueing_delay_LocalEdgeBrokerDomainRoute_t1_subscription_session_eb1.csv"
data=np.genfromtxt(file_name,dtype='int',delimiter=',',\
        usecols=[2],skip_header=1)[1000:] 
print(np.mean(np.absolute(data-10)))
print(np.mean(data))
