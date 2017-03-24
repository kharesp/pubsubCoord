import argparse,infrastructure,experiment3,time

def create_conf(num_topics,locality):
  num_routing_brokers=5
  num_edge_brokers=5
  num_clients_per_region=10
  max_endpoints_per_client=10

  num_local_topics= int(num_topics*float(locality))
  num_global_topics=num_topics-num_local_topics
  topics=['t-%d-%d'%(region+1,topic+1) for topic in range(num_global_topics)\
    for region in range(num_edge_brokers) ]
  topics.extend(['t-l-%d-%d'%(region+1,topic+1) for \
    topic in range(num_local_topics) for region in range(num_edge_brokers)])

  rbs=['rb%d'%(i+1) for i in range(num_routing_brokers)]
  ebs=['eb%d'%(i+1) for i in range(num_edge_brokers)]
 
  num_clients=num_local_topics//max_endpoints_per_client
  remaining= num_local_topics%max_endpoints_per_client

  if num_clients==0:
    sub_distribution=['cli%d-1:t-l-%d-%d:1'%(region+1,region+1,topic+1) \
      for topic in range(remaining)\
      for region in range(num_edge_brokers)]
    pub_distribution=['cli%d-1:t-l-%d-%d:1'%(region+1,region+1,topic+1) \
      for topic in range(remaining)\
      for region in range(num_edge_brokers)]
    clients=['cli%d-1'%(region+1) for region in range(num_edge_brokers)]
  else:  
    sub_distribution=['cli%d-%d:t-l-%d-%d:1'%(region+1,cli+1,\
      region+1,cli*max_endpoints_per_client+topic+1) \
      for topic in range(max_endpoints_per_client)\
      for cli in range(num_clients)\
      for region in range(num_edge_brokers)]
    pub_distribution=['cli%d-%d:t-l-%d-%d:1'%(region+1,cli+1,\
      region+1,cli*max_endpoints_per_client+topic+1) \
      for topic in range(max_endpoints_per_client)\
      for cli in range(num_clients)\
      for region in range(num_edge_brokers)]
    clients=['cli%d-%d'%(region+1,cli+1) for cli in range(num_clients)\
      for region in range(num_edge_brokers)]

    if (remaining>0): 
      for region in range(num_edge_brokers):
        clients.append('cli%d-%d'%(region+1,num_clients+1))
        for topic in range(remaining):
          sub_distribution.append('cli%d-%d:t-l-%d-%d:1'%\
            (region+1,num_clients+1,region+1,num_clients*max_endpoints_per_client+topic+1))
          pub_distribution.append('cli%d-%d:t-l-%d-%d:1'%\
            (region+1,num_clients+1,region+1,num_clients*max_endpoints_per_client+topic+1))


  #Assign global topic pub-sub pairs
  pairs_left_to_accommodate=0
  if (remaining>0):
    pairs_left_to_accommodate=max_endpoints_per_client-remaining
    num_global_topics-= pairs_left_to_accommodate
    for region in range(num_edge_brokers):
      for topic in range(pairs_left_to_accommodate):
        pub_distribution.append('cli%d-%d:t-%d-%d:1'%\
          (region+1,num_clients+1,region+1,topic+1))  
        if (region==0):
          subscribing_region=num_edge_brokers
        else:
          subscribing_region=region
        sub_distribution.append('cli%d-%d:t-%d-%d:1'%\
          (region+1,num_clients+1,subscribing_region,topic+1))  

  #assignment of global topics
  if (num_global_topics > 0):
    starting_index_of_global_topic=pairs_left_to_accommodate+1 
    starting_index_of_client_machine= (num_clients +1) if \
      (remaining==0) else (num_clients+2)

    num_clients_global= num_global_topics//max_endpoints_per_client
    remaining_global= num_global_topics % max_endpoints_per_client


    if (num_clients_global==0):
      for region in range(num_edge_brokers):
        clients.append('cli%d-%d'%(region+1,starting_index_of_client_machine))
        for topic in range(remaining_global):
          pub_distribution.append('cli%d-%d:t-%d-%d:1'%\
            (region+1,starting_index_of_client_machine,\
            region+1,starting_index_of_global_topic+topic))
          if (region==0):
            subscribing_region=num_edge_brokers
          else:
            subscribing_region=region
          sub_distribution.append('cli%d-%d:t-%d-%d:1'%\
            (region+1,starting_index_of_client_machine,\
            subscribing_region,starting_index_of_global_topic+topic))
    else:
      for region in range(num_edge_brokers):
        for cli in range(num_clients_global):
          clients.append('cli%d-%d'%(region+1,starting_index_of_client_machine+cli))
          for topic in range(max_endpoints_per_client):
            pub_distribution.append('cli%d-%d:t-%d-%d:1'%\
              (region+1,starting_index_of_client_machine+cli,\
                region+1,starting_index_of_global_topic+cli*max_endpoints_per_client+topic))
            if (region==0):
              subscribing_region=num_edge_brokers
            else:
              subscribing_region=region
            sub_distribution.append('cli%d-%d:t-%d-%d:1'%\
              (region+1,starting_index_of_client_machine+cli,subscribing_region,\
              starting_index_of_global_topic+cli*max_endpoints_per_client+topic))

      if (remaining_global>0):
        for region in range(num_edge_brokers):
          clients.append('cli%d-%d'%(region+1,\
            starting_index_of_client_machine+num_clients_global))
          for topic in range(remaining_global):
            pub_distribution.append('cli%d-%d:t-%d-%d:1'%\
              (region+1,starting_index_of_client_machine+num_clients_global,region+1,\
              starting_index_of_global_topic+num_clients_global*max_endpoints_per_client+topic))
            if (region==0):
              subscribing_region=num_edge_brokers
            else:
              subscribing_region=region
            sub_distribution.append('cli%d-%d:t-%d-%d:1'%\
              (region+1,starting_index_of_client_machine+num_clients_global,subscribing_region,\
              starting_index_of_global_topic+num_clients_global*max_endpoints_per_client+topic))

  conf="""run_id:%d/locality_%s
rbs:%s
ebs:%s
clients:%s
topics:%s
no_subs:%d
no_pubs:%d
sub_distribution:%s
pub_distribution:%s
pub_sample_count:5000
sub_sample_count:5000
sleep_interval:50"""%(num_topics,locality,','.join(rbs),','.join(ebs),','.join(clients),\
  num_topics*num_edge_brokers,num_topics*num_edge_brokers,num_topics*num_edge_brokers,\
  ','.join(sub_distribution),','.join(pub_distribution))
  print(conf)
  with open('conf/conf.csv','w') as f:
    f.write(conf)

def run(min_topics,step_size,max_topics,kill):
  locality=['.6','.8']
  for num in range(min_topics,max_topics+step_size,step_size):
    for l in locality:
      create_conf(num,l)
      infrastructure.Infrastructure('conf/conf.csv',True).setup()
      experiment3.Experiment('conf/conf.csv','%d/locality_%s'%(num,l),kill).run()
      time.sleep(2)
      

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting data locality test')
  parser.add_argument('min_topics',type=int,help='minimum number of topics to start from')
  parser.add_argument('step_size',type=int,help='step size with with to iterate from min_topics to max_topics')
  parser.add_argument('max_topics',type=int,help='maximum number of topics')
  parser.add_argument('--kill',dest='kill',action='store_true',help='flag to determine whether to kill pre-existing client and monitoirng processes.')
  args=parser.parse_args()

  run(args.min_topics,args.step_size,args.max_topics,args.kill)
