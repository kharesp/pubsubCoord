import matplotlib.pyplot as plt

locality=[.1,.4,.9]
cpu_topics_10=[4.703746,4.419891,4.009001]
cpu_topics_20=[7.209208,6.687221,5.867323]

mem_topics_10=[151.311616,139.593815,130.225969]
mem_topics_20=[212.748970,199.791960,178.749224]

ax=plt.subplot(2,1,1)
ax.set_xticks(locality)
ax.set_ylabel('EB host cpu utilization (%)')

ax.plot(locality,cpu_topics_10,marker='*',label='#topics 10')
ax.plot(locality,cpu_topics_20,marker='d',label='#topics 20')
ax.yaxis.grid(True)
ax.legend()

ax=plt.subplot(2,1,2)
ax.set_xlabel('data locality')
ax.set_xticks(locality)
ax.set_ylabel('EB host memory utilization (mb)')

ax.plot(locality,mem_topics_10,marker='*',label='#topics 10')
ax.plot(locality,mem_topics_20,marker='d',label='#topics 20')
ax.yaxis.grid(True)
ax.legend()

plt.tight_layout()
plt.savefig('/home/kharesp/workspace/ansible/pubsubCoord/logs/test2.pdf',\
  bbox_inches='tight')
plt.close()
