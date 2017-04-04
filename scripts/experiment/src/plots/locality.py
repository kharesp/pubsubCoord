import matplotlib.pyplot as plt

locality=[.1,.4,.9]
latency_topics_10=[126.721555,103.454381,39.351047]
latency_topics_20=[189.621288,93.556060,41.328961]

fig,ax = plt.subplots()
ax.set_xlabel('data locality')
ax.set_xticks(locality)
ax.set_ylabel('latency (ms)')

ax.plot(locality,latency_topics_10,marker='*',label='#topics 10')
ax.plot(locality,latency_topics_20,marker='d',label='#topics 20')
ax.yaxis.grid(True)
ax.legend()

plt.savefig('/home/kharesp/workspace/ansible/pubsubCoord/logs/test.pdf',\
  bbox_inches='tight')
plt.close()
