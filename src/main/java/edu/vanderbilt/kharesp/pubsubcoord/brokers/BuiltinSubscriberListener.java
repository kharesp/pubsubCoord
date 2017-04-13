package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import com.rti.dds.infrastructure.RETCODE_NO_DATA;
import com.rti.dds.subscription.DataReader;
import com.rti.dds.subscription.DataReaderAdapter;
import com.rti.dds.subscription.InstanceStateKind;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.builtin.SubscriptionBuiltinTopicData;
import com.rti.dds.subscription.builtin.SubscriptionBuiltinTopicDataDataReader;
import edu.vanderbilt.kharesp.pubsubcoord.routing.RoutingService;
import edu.vanderbilt.kharesp.pubsubcoord.routing.RoutingServiceAdministrator;
import edu.vanderbilt.kharesp.pubsubcoord.routing.TopicSession;

public class BuiltinSubscriberListener extends DataReaderAdapter {
    private boolean emulated_broker;
	private String ebAddress;
	
	//domain route names at this EB
	private String domainRouteName;
	private String subDomainRouteName;

	private Logger logger;
	private CuratorHelper client;
	private RoutingServiceAdministrator rs;

	private SubscriptionBuiltinTopicData subscription_builtin_topic_data = new SubscriptionBuiltinTopicData();
	private SampleInfo info= new SampleInfo();
	
	//map to keep track of number of subscribers for each topic
	private HashMap<String, Integer> topic_subscriberCount_map= new HashMap<>();

	//map to keep track of which instance handle belongs to which topic
	private HashMap<String,String> instanceHandle_topic_map= new HashMap<String,String>();

	//map to keep track of topic node caches for different topics operational in this local domain
	private HashMap<String, NodeCache> topic_topicCache_map = new HashMap<String, NodeCache>();

	//map to keep track of RBs this local domain is interfacing with and for which topics
	private HashMap<String,HashSet<String>> rb_topics_map=new HashMap<String,HashSet<String>>();

	public BuiltinSubscriberListener(String ebAddress,CuratorHelper client,
			RoutingServiceAdministrator rs,boolean emulated_broker){
		this.ebAddress=ebAddress;
		domainRouteName=EdgeBroker.DOMAIN_ROUTE_NAME_PREFIX+"@"+ebAddress;
		subDomainRouteName=EdgeBroker.SUB_DOMAIN_ROUTE_NAME_PREFIX+"@"+ebAddress;
		logger=LogManager.getLogger(this.getClass().getSimpleName());
		this.rs=rs;
		this.client=client;
		this.emulated_broker=emulated_broker;
	}

	public void on_data_available(DataReader reader) {
		SubscriptionBuiltinTopicDataDataReader builtin_reader = (SubscriptionBuiltinTopicDataDataReader) reader;
		try {
			while (true) {
				builtin_reader.take_next_sample(subscription_builtin_topic_data, info);
				if (info.instance_state == InstanceStateKind.ALIVE_INSTANCE_STATE) {
					//Discovered a new Subscriber in this region
					logger.debug("Built-in Reader: found subscriber: "+
							"\n\ttopic_name->"
							+ subscription_builtin_topic_data.topic_name +
							"\n\tinstance_handle->"+
							info.instance_handle.toString());
					add_subscriber();
				}
				if (info.instance_state == InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE
						|| info.instance_state == InstanceStateKind.NOT_ALIVE_NO_WRITERS_INSTANCE_STATE) {
					//Subscriber was deleted in this region
					logger.debug(
							"Built-in Reader: subscriber was deleted:" + 
					        "\n\tinstance_handle->"+
						    info.instance_handle.toString());
					delete_subscriber();
				}
			}
		} catch (RETCODE_NO_DATA noData) {
			return;
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}

	}

	private void add_subscriber(){
		 String userData =
                 new String(subscription_builtin_topic_data.user_data.value.toArrayByte(null));

         //Process only if this subscriber is a client subscriber in our local domain 
         if (!(userData.equals(RoutingService.INFRASTRUCTURE_NODE_IDENTIFIER))) {
        	 //Cache topic name of discovered Subscriber's topic 
        	 String topic=subscription_builtin_topic_data.topic_name.replaceAll("\\s", "");

        	 //Add this instance handle to instanceHandle_topic map
        	 instanceHandle_topic_map.put(info.instance_handle.toString(), topic);

        	 //Update the current count of subscribers for this topic in the domain
        	 if (topic_subscriberCount_map.containsKey(topic)) {
        		 topic_subscriberCount_map.put(topic, topic_subscriberCount_map.get(topic) + 1);
        	 } else {
        		 topic_subscriberCount_map.put(topic, 1);
        	 }
        	 
        	 logger.debug(String.format("Current subscriber count for topic:%s is %d\n",
        			 topic,topic_subscriberCount_map.get(topic)));
        

        	 //Create topic path for topic t if it does not already exist
        	 if (topic_subscriberCount_map.get(topic)==1){
        		 //create this EB's znode under /topics/t/sub to denote this region's interest in topic t
        		 create_EB_znode(topic,subscription_builtin_topic_data);

        		 logger.debug(String.format("Creating topic session for topic:%s\n",topic));
        		 //create topic session
        		 create_topic_session(subscription_builtin_topic_data.topic_name,
        				 subscription_builtin_topic_data.type_name);

        		 //Install listener for RB assignment for topic t
        		 install_topic_to_rb_assignment_listener(topic);
        	 }
        	
         } else {
        	 logger.debug("This subscriber was created by an infrastructure entity\n");
         }
	}
	

	private void delete_subscriber(){
		
		//retrieve the topic name for which this subscriber was removed
		String topic=instanceHandle_topic_map.getOrDefault(info.instance_handle.toString(),null);
		if (topic==null){
			logger.debug("This subscriber was created by RS\n");
			return;
		}
		
		//remove the instance handle from instanceHandle_topic_map 
		instanceHandle_topic_map.remove(info.instance_handle.toString());

		//update the current count of subscribers for this topic
		int updated_count=topic_subscriberCount_map.get(topic)-1;

		logger.debug(String.format("Current subscriber count for topic:%s is %d\n",
   			 topic,updated_count));

		if (updated_count==0){
			topic_subscriberCount_map.remove(topic);
		}else{
			topic_subscriberCount_map.put(topic, updated_count);
		}
		
   	 	//Remove EB znode if subscriber count==0
   	 	if (updated_count==0){
			SubscriptionBuiltinTopicData subscription_builtin_topic_data = delete_EB_znode(topic);
   	 		
			//Remove topic session if subscriber count is 0
   	 		logger.debug(String.format("Removing topic session for %s as subscriber count is 0\n", topic));
   	 		if(emulated_broker){
   	 			rs.deleteTopicSession(subDomainRouteName,subscription_builtin_topic_data.topic_name,
   	 					subscription_builtin_topic_data.type_name,TopicSession.PUBLICATION_SESSION);
   	 		}else{
   	 			rs.deleteTopicSession(domainRouteName,subscription_builtin_topic_data.topic_name,
   	 					subscription_builtin_topic_data.type_name,TopicSession.PUBLICATION_SESSION);
   	 		}
   	 	
   	 		//Remove listener for RB assignment if subscriber count=0
   	 		NodeCache topicCache=topic_topicCache_map.remove(topic);
   	 		String RB_address= new String(topicCache.getCurrentData().getData());
   	 		String topic_path= topicCache.getCurrentData().getPath();
   	 		logger.debug(String.format("Removing listener for RB assignment for topic node:%s as subscriber count is 0\n",
   	 				topic_path));
   	 		topicCache.getListenable().clear();
   	 		try {
				topicCache.close();
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
   	 		
   	 		//If we are not interacting with this RB for any other topic, then remove RB as peer
   	 		HashSet<String> topics= rb_topics_map.getOrDefault(RB_address,null);
   	 		if (topics!=null){
   	 			topics.remove(topic_path);
   	 			if(topics.size()==0){
   	 				//remove RB as peer 
   	 				logger.debug(String.format("Local domain is not connected to RB:%s for any topics.\n"
   	 						+ "Hence, removing RB:%s as peer.\n",RB_address,RB_address));
   	 				String rbLocator = "tcpv4_wan://" + RB_address + ":" + RoutingBroker.RB_P2_BIND_PORT;
   	 				if(emulated_broker){
   	 					rs.removePeer(subDomainRouteName,rbLocator,false);
   	 				}else{
   	 					//TODO: For non-emulated scenario, there is only one domain route and removing RB
   	 					//as peer will depend on whether there are any publishers interested in any of the topics
   	 					//hosted on RB in this region.
   	 				}
   	 				rb_topics_map.remove(RB_address);
   	 			}
   	 		}
   	 		
   	 	}
	}

	private void create_EB_znode(String topic, SubscriptionBuiltinTopicData subscription_builtin_data){
		//ensure topic path /topics/t/sub exists
		String parent_path= (CuratorHelper.TOPIC_PATH+"/"+topic+"/sub");
		client.create(parent_path,new byte[0],CreateMode.PERSISTENT);

		//Create this EB's znode at /topics/t/sub/ebAddress to signify this region's interest in topic t
		client.create(parent_path+"/"+ebAddress, subscription_builtin_data, CreateMode.PERSISTENT);
		logger.debug(String.format("Created EB znode for EB:%s for topic path:%s\n",
				ebAddress,parent_path));
	}
	
	

	private SubscriptionBuiltinTopicData  delete_EB_znode(String topic){
		SubscriptionBuiltinTopicData subscription_builtin_topic_data=null;
		String parent_path= (CuratorHelper.TOPIC_PATH+"/"+topic+"/sub");
		String znode_name= ebAddress;
		String path=ZKPaths.makePath(parent_path, znode_name);

		try {
			subscription_builtin_topic_data=
					(SubscriptionBuiltinTopicData)client.get(path);

			//delete this EB's znode under /topics/t/sub/ebAddress
			client.delete(path);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		logger.debug(String.format("Deleted EB:%s under topic path:%s as"
				+ " number of subscribers for topic:%s is 0\n",
				ebAddress,parent_path,topic));
		return subscription_builtin_topic_data;
	}

	private void install_topic_to_rb_assignment_listener(String topic){
		//This topic's path under zk
		String topic_path=CuratorHelper.TOPIC_PATH+"/"+topic;

		if (!topic_topicCache_map.containsKey(topic)){
			//Create node cache for this topic's znode
			NodeCache topicCache= client.nodeCache(topic_path);
			topic_topicCache_map.put(topic, topicCache);

			//Install listener to listen for RB assignment for this topic
			logger.debug(String.format("Installing listener for topic:%s to listen for RB assignment\n",
					topic));
			topicCache.getListenable().addListener(new NodeCacheListener(){

				@Override
				public void nodeChanged() throws Exception {
					//RB to which this topic was assigned
					 String rb_address = new String(topicCache.getCurrentData().getData());
					 String topic_path= topicCache.getCurrentData().getPath();
					 if (!rb_address.isEmpty()) {
						 logger.debug(String.format("Topic:%s was assigned to RB:%s\n",topic_path,rb_address));

						//Check if we are interfacing with this RB already for some other topics
						 if (!rb_topics_map.containsKey(rb_address)) {
							 rb_topics_map.put(rb_address,new HashSet<String>());
							 rb_topics_map.get(rb_address).add(topic_path);
							 
							//We are not interfacing with this RB for any other topics, so add this RB as peer
							 String rbLocator = "tcpv4_wan://" + rb_address + ":" + RoutingBroker.RB_P2_BIND_PORT;
							 logger.debug(String.format("Adding RB:%s as peer\n",rbLocator));
							 if(emulated_broker){
								 rs.addPeer(subDomainRouteName,rbLocator, false);
							 }else{
								 rs.addPeer(domainRouteName,rbLocator, false);
							 }
						 }else{
							 //RB already exists as peer
							 HashSet<String> topics=rb_topics_map.get(rb_address);
							 topics.add(topic_path);
							 logger.debug(String.format("RB:%s for topic:%s already exists as peer\n",
									 rb_address,topic_path));
						 }
					 }
				}
			});
			try {
				//start topic cache
				topicCache.start();
			} catch (Exception e) {
				logger.error(e.getMessage(),e);
			}
		}
	}

	private void create_topic_session(String topic_name,String type_name){
		if(emulated_broker){
			rs.createTopicSession(subDomainRouteName,topic_name,type_name,TopicSession.PUBLICATION_SESSION); 
		}
		else{
			rs.createTopicSession(domainRouteName,topic_name,type_name, TopicSession.PUBLICATION_SESSION);
		}
	}

}
