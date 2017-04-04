import matplotlib.pyplot as plt

locality=[.1,.3,.5,.7,.9]
cpu_topics_100=[28.588215,27.988040,24.860503,24.059399,22.257595]
cpu_topics_80=[25.166563,23.791674,22.008478,21.010630,19.676134]
cpu_topics_60=[21.366994,21.010630,19.051853,17.866251,16.827171]

mem_topics_100=[753.348769,722.823880,686.941892,650.605384,610.190720]
mem_topics_80=[667.853461,642.045889,612.355548,577.879581,544.609171]
mem_topics_60=[570.894539,544.383063,515.126802,493.044802,460.499517]

ax=plt.subplot(2,1,1)
ax.set_xticks(locality)
ax.set_ylabel('EB host cpu utilization (%)')

ax.plot(locality,cpu_topics_100,marker='s',label='#topics 100')
ax.plot(locality,cpu_topics_80,marker='d',label='#topics 80')
ax.plot(locality,cpu_topics_60,marker='*',label='#topics 60')
ax.yaxis.grid(True)
ax.legend()

ax=plt.subplot(2,1,2)
ax.set_xlabel('data locality')
ax.set_xticks(locality)
ax.set_ylabel('EB host memory utilization (mb)')

ax.plot(locality,mem_topics_100,marker='s',label='#topics 100')
ax.plot(locality,mem_topics_80,marker='d',label='#topics 80')
ax.plot(locality,mem_topics_60,marker='*',label='#topics 60')
ax.yaxis.grid(True)
ax.legend()

plt.tight_layout()
plt.savefig('/home/kharesp/workspace/ansible/pubsubCoord/logs/test2.pdf',\
  bbox_inches='tight')
plt.close()
