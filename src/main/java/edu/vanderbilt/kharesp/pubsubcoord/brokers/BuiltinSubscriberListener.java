package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.curator.framework.CuratorFramework;
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

public class BuiltinSubscriberListener extends DataReaderAdapter {

	//Constant String Value to identify endpoints created by RS
	private static final String TOPIC_ROUTE_CODE = "107"; 
    private static final String TOPIC_ROUTE_STRING_CODE = "k";
    
    //Port at which RBs send out data for subscribers 
    private static final String RB_P2_BIND_PORT = "8501";
    
    //This local domain's DomainRouteName's Prefix
    private static final String DOMAIN_ROUTE_NAME_PREFIX = "EdgeBrokerDomainRoute";

	private String ebAddress;
	private String domainRouteName;
	private Logger logger;
	private CuratorFramework client;
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

	public BuiltinSubscriberListener(String ebAddress,CuratorFramework client, RoutingServiceAdministrator rs){
		this.ebAddress=ebAddress;
		domainRouteName=DOMAIN_ROUTE_NAME_PREFIX+"@"+ebAddress;
		logger=LogManager.getLogger(this.getClass().getSimpleName());
		this.rs=rs;
		this.client=client;
	}

	public void on_data_available(DataReader reader) {
		SubscriptionBuiltinTopicDataDataReader builtin_reader = (SubscriptionBuiltinTopicDataDataReader) reader;
		try {
			while (true) {
				builtin_reader.take_next_sample(subscription_builtin_topic_data, info);
				if (info.instance_state == InstanceStateKind.ALIVE_INSTANCE_STATE) {
					logger.debug("Built-in Reader: found subscriber: "+
							"\n\ttopic_name->"
							+ subscription_builtin_topic_data.topic_name +
							"\n\tinstance_handle->"+
							info.instance_handle.toString());
					add_subscriber();
				}
				if (info.instance_state == InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE
						|| info.instance_state == InstanceStateKind.NOT_ALIVE_NO_WRITERS_INSTANCE_STATE) {
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
		}

	}
	private void add_subscriber(){
		 String userData =
                 new String(subscription_builtin_topic_data.user_data.value.toArrayByte(null));

         //Process only if this subscriber is a client subscriber in our local domain 
         if (!(userData.equals(TOPIC_ROUTE_STRING_CODE))) {
        	 //Cache topic name of discovered Subscriber's topic 
        	 String topic=subscription_builtin_topic_data.topic_name.replaceAll("\\s", "");

        	 //Add this instance handle to topic mapping to instanceHandle_topic map
        	 instanceHandle_topic_map.put(info.instance_handle.toString(), topic);

        	 //Update the current count of subscribers for this topic in the domain
        	 if (topic_subscriberCount_map.containsKey(topic)) {
        		 topic_subscriberCount_map.put(topic, topic_subscriberCount_map.get(topic) + 1);
        	 } else {
        		 topic_subscriberCount_map.put(topic, 1);
        	 }
        	 
        	 logger.debug(String.format("Current subscriber count for topic:%s is %d\n",
        			 topic,topic_subscriberCount_map.get(topic)));
        
        	 if(topic_subscriberCount_map.get(topic)==1){
        		 create_EB_znode(topic,subscription_builtin_topic_data.type_name);
        	 }

        	 //Create topic path for topic t if it does not already exist
        	 if (topic_subscriberCount_map.get(topic)==1){
        		 logger.debug(String.format("Creating topic path for topic:%s\n",topic));
        		 create_topic_path(topic,subscription_builtin_topic_data.type_name);
        	 }
        	 
        	 //Install listener for RB assignment for topic t
        	 install_topic_to_rb_assignment_listener(topic);
        	
         	} else {
         		logger.debug("This subscriber was created by RS\n");
         	}
	}
	
	

	private void delete_subscriber(){
		//retrieve the topic name for which this subscriber was removed
		String topic=instanceHandle_topic_map.get(info.instance_handle.toString());
		//remove the instance handle 
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
		
		//delete EB for this domain under /topics/t/sub when number of subscribers for t is 0
		if (updated_count==0){
   	 		delete_EB_znode(topic);
		}
   	 	
   	 	//Remove topic path if subscriber count==0
   	 	if (updated_count==0){
   	 		logger.debug(String.format("Removing topic route for %s as subscriber count is 0\n", topic));
   	 		rs.deleteTopicRoute(String.format("%s::%sPublicationSession::%sPublicationRoute",
   	 				domainRouteName,topic,topic));
   	 	}
   	 	
   	 	if(updated_count==0){
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
   	 			/*if(topics.size()==0){
   	 				//remove RB as peer 
   	 				logger.debug(String.format("Local domain is not connected to RB:%s for any topics.\n"
   	 						+ "Hence, removing RB:%s as peer.\n",RB_address,RB_address));
   	 				String rbLocator = "tcpv4_wan://" + RB_address + ":" + RB_P2_BIND_PORT;
   	 				rs.removePeer(domainRouteName,rbLocator,false);
   	 				rb_topics_map.remove(RB_address);
   	 			}*/
   	 		}
   	 		
   	 	}
	}
	private void create_EB_znode(String topic, String type_name){
		String parent_path= (CuratorHelper.TOPIC_PATH+"/"+topic+"/sub");
		String znode_name= ebAddress;
		String path=ZKPaths.makePath(parent_path, znode_name);
		try {
			client.create().
				creatingParentsIfNeeded().
				withMode(CreateMode.PERSISTENT).
				forPath(path, type_name.getBytes());
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		logger.debug(String.format("Created EB znode for EB:%s for topic path:%s\n",
				ebAddress,parent_path));
	}
	private void delete_EB_znode(String topic){
		String parent_path= (CuratorHelper.TOPIC_PATH+"/"+topic+"/sub");
		String znode_name= ebAddress;
		String path=ZKPaths.makePath(parent_path, znode_name);
		try {
			client.delete().forPath(path);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		logger.debug(String.format("Deleted EB:%s under topic path:%s as"
				+ " number of subscribers for topic:%s is 0\n",
				ebAddress,parent_path,topic));
	}

	
	private void install_topic_to_rb_assignment_listener(String topic){
		String topic_path=CuratorHelper.TOPIC_PATH+"/"+topic;
		if (!topic_topicCache_map.containsKey(topic)){
			NodeCache topicCache= new NodeCache(client,topic_path);
			topic_topicCache_map.put(topic, topicCache);


			logger.debug(String.format("Installing listener for topic:%s to listen for RB assignment\n",
					topic));
			topicCache.getListenable().addListener(new NodeCacheListener(){

				@Override
				public void nodeChanged() throws Exception {
					 String rb_address = new String(topicCache.getCurrentData().getData());
					 String topic_path= topicCache.getCurrentData().getPath();
					 if (!rb_address.isEmpty()) {
						 logger.debug(String.format("Topic:%s was assigned to RB:%s\n",topic_path,rb_address));

						 if (!rb_topics_map.containsKey(rb_address)) {
							 rb_topics_map.put(rb_address,new HashSet<String>());
							 rb_topics_map.get(rb_address).add(topic_path);
							 String rbLocator = "tcpv4_wan://" + rb_address + ":" + RB_P2_BIND_PORT;
							 logger.debug(String.format("Adding RB:%s as peer\n",rbLocator));
							 rs.addPeer(domainRouteName,rbLocator, false);
						 }else{
							 HashSet<String> topics=rb_topics_map.get(rb_address);
							 topics.add(topic_path);
						 }
					 }
				}
			});
			try {
				topicCache.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void create_topic_path(String topic_name,String type_name){
		rs.createTopicRoute(domainRouteName, 
				 "str://\"<session name=\"" + topic_name + "PublicationSession\">" +
                         "<topic_route name=\"" + topic_name + "PublicationRoute\">" +
                         "<route_types>true</route_types>" +
                         "<publish_with_original_info>true</publish_with_original_info>" +
                         "<publish_with_original_timestamp>true</publish_with_original_timestamp>" +
                         "<input participant=\"2\">" +
                         "<topic_name>" + topic_name + "</topic_name>" +
                         "<registered_type_name>" + type_name + "</registered_type_name>" +
                         "<creation_mode>IMMEDIATE</creation_mode>" +
                         "<datareader_qos>" +
                         "<reliability>" +
                         "<kind>RELIABLE_RELIABILITY_QOS</kind>" +
                         "</reliability>" +
                         "<durability>" +
                         "<kind>TRANSIENT_LOCAL_DURABILITY_QOS</kind>" +
                         "</durability>" +
                         "<history>" +
                         "<kind>KEEP_ALL_HISTORY_QOS</kind>" +
                         "</history>" +
                         "<user_data><value>" + TOPIC_ROUTE_CODE + "</value></user_data>" +
                         "</datareader_qos>" +
                         "</input>" +
                         "<output>" +
                         "<topic_name>" + topic_name + "</topic_name>" +
                         "<registered_type_name>" + type_name + "</registered_type_name>" +
                         "<creation_mode>IMMEDIATE</creation_mode>" +
                         "<datawriter_qos>" +
                         "<reliability>" +
                         "<kind>RELIABLE_RELIABILITY_QOS</kind>" +
                         "</reliability>" +
                         "<durability>" +
                         "<kind>TRANSIENT_LOCAL_DURABILITY_QOS</kind>" +
                         "</durability>" +
                         "<history>" +
                         "<kind>KEEP_ALL_HISTORY_QOS</kind>" +
                         "</history>" +
                         "<lifespan>" +
                         "<duration>" +
                         "<sec>300</sec>" +
                         "<nanosec>0</nanosec>" +
                         "</duration>" +
                         "</lifespan>" +
                         "<user_data><value>" + TOPIC_ROUTE_CODE + "</value></user_data>" +
                         "</datawriter_qos>" +
                         "</output>" +
                         "</topic_route>" +
                         "</session>\"");
	}

}
