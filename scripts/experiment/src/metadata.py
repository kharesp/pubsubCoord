ansible='/home/kharesp/workspace/ansible/pubsubCoord'
zk='129.59.107.97:2181'
leader_path='/leader'
topics_path='/topics'
rb_path='/routingBrokers'
experiment_path='/experiment'
initial_samples=1500
max_subscribers_per_host=15
max_publishers_per_host=10

latency_summary_header='topic,#subscribers,\
mean latency(ms),std_dev latency(ms),min latency(ms),max latency(ms),\
90%ile latency(ms),99%ile latency(ms),99.9%ile latency(ms),\
99.99%ile latency(ms),99.999%ile latency(ms),99.9999%ile latency(ms),\
mean interarrival(ms),std_dev interarrival(ms),min interarrival(ms),max interarrival(ms),\
90%ile interarrival(ms),99%ile interarrival(ms),99.9%ile interarrival(ms),\
99.99%ile interarrival(ms),99.999%ile interarrival(ms),99.9999%ile interarrival(ms)\n'

rs_summary_header='broker,cpu mean(%),cpu std_dev(%),cpu min(%),cpu max(%),\
host cpu mean(%),host cpu std_dev(%),host cpu min(%),host cpu max(%),mem mean(kb),mem std_dev(kb),mem min(kb),mem max(kb)\n'
