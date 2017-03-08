import matplotlib.pyplot as plt

locality=[.1,.4,.9]
cpu_topics_10=[29.985221,20.669762,5.531660]
cpu_topics_20=[44.214266,31.444136,8.729994]

mem_topics_10=[676.052267,447.688152,112.685071]
mem_topics_20=[1014.255703,703.495548,157.477734]

ax=plt.subplot(2,1,1)
ax.set_xticks(locality)
ax.set_ylabel('RB host cpu utilization (%)')

ax.plot(locality,cpu_topics_10,marker='*',label='#topics 10')
ax.plot(locality,cpu_topics_20,marker='d',label='#topics 20')
ax.yaxis.grid(True)
ax.legend()

ax=plt.subplot(2,1,2)
ax.set_xlabel('data locality')
ax.set_xticks(locality)
ax.set_ylabel('RB host memory utilization (mb)')

ax.plot(locality,mem_topics_10,marker='*',label='#topics 10')
ax.plot(locality,mem_topics_20,marker='d',label='#topics 20')
ax.yaxis.grid(True)
ax.legend()

plt.tight_layout()
plt.savefig('/home/kharesp/workspace/ansible/pubsubCoord/logs/test3.pdf',\
  bbox_inches='tight')
plt.close()
