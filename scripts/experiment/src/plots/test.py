import matplotlib.pyplot as plt

locality=[.1,.3,.5,.7,.9]
latency_topics_100=[152.396626,143.565002,120.610839,94.122485,65.611247]
latency_topics_80=[143.258525,135.419696,117.216917,94.345617,60.427115]
latency_topics_60=[138.483047,133.859850,114.633023,90.115153,49.016102]


fig,ax = plt.subplots()
ax.set_xlabel('data locality')
ax.set_xticks(locality)
#ax.invert_xaxis()
ax.set_ylabel('latency (ms)')

ax.plot(locality,latency_topics_100,marker='s',label='#topics 100')
ax.plot(locality,latency_topics_80,marker='d',label='#topics 80')
ax.plot(locality,latency_topics_60,marker='*',label='#topics 60')
ax.yaxis.grid(True)
ax.legend()

plt.savefig('/home/kharesp/workspace/ansible/pubsubCoord/logs/test.pdf',\
  bbox_inches='tight')
plt.close()
