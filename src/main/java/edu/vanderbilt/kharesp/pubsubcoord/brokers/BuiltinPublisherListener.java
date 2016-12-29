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
import com.rti.dds.publication.builtin.PublicationBuiltinTopicData;
import com.rti.dds.publication.builtin.PublicationBuiltinTopicDataDataReader;
import com.rti.dds.subscription.DataReader;
import com.rti.dds.subscription.DataReaderAdapter;
import com.rti.dds.subscription.InstanceStateKind;
import com.rti.dds.subscription.SampleInfo;

public class BuiltinPublisherListener extends DataReaderAdapter {

	//Constant String Value to identify endpoints created by RS
	private static final String TOPIC_ROUTE_CODE = "107"; 
    private static final String TOPIC_ROUTE_STRING_CODE = "k";
    
    //Port at which RBs listen for incoming/publisher data
    private static final String RB_P1_BIND_PORT = "8500";
    
    //This local domain's DomainRouteName's Prefix
    private static final String PUB_DOMAIN_ROUTE_NAME_PREFIX = "PubEdgeBrokerDomainRoute";
    private static final String LOCAL_DOMAIN_ROUTE_NAME_PREFIX = "LocalEdgeBrokerDomainRoute";

	private String ebAddress;
	private String pubDomainRouteName;
	private String localDomainRouteName;
	private Logger logger;
	private CuratorFramework client;
	private RoutingServiceAdministrator rs;

	private PublicationBuiltinTopicData publication_builtin_topic_data = new PublicationBuiltinTopicData();
	private SampleInfo info= new SampleInfo();
	
	//map to keep track of number of publishers for each topic
	private HashMap<String, Integer> topic_publisherCount_map= new HashMap<>();
	//map to keep track of which instance handle belongs to which topic
	private HashMap<String,String> instanceHandle_topic_map= new HashMap<String,String>();
	//map to keep track of topic node caches for different topics operational in this local domain
	private HashMap<String, NodeCache> topic_topicCache_map = new HashMap<String, NodeCache>();
	//map to keep track of RBs this local domain is interfacing with and for which topics
	private HashMap<String,HashSet<String>> rb_topics_map=new HashMap<String,HashSet<String>>();

	public BuiltinPublisherListener(String ebAddress,CuratorFramework client, RoutingServiceAdministrator rs){
		this.ebAddress=ebAddress;
		localDomainRouteName=LOCAL_DOMAIN_ROUTE_NAME_PREFIX+"@"+ebAddress;
		pubDomainRouteName=PUB_DOMAIN_ROUTE_NAME_PREFIX+"@"+ebAddress;
		logger=LogManager.getLogger(this.getClass().getSimpleName());
		this.rs=rs;
		this.client=client;
	}

	public void on_data_available(DataReader reader) {
		PublicationBuiltinTopicDataDataReader builtin_reader = (PublicationBuiltinTopicDataDataReader) reader;
		try {
			while (true) {
				builtin_reader.take_next_sample(publication_builtin_topic_data, info);
				if (info.instance_state == InstanceStateKind.ALIVE_INSTANCE_STATE) {
					logger.debug("Built-in Reader: found publisher: "+
							"\n\ttopic_name->"
							+ publication_builtin_topic_data.topic_name +
							"\n\tinstance_handle->"+
							info.instance_handle.toString());
					add_publisher();
				}
				if (info.instance_state == InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE
						|| info.instance_state == InstanceStateKind.NOT_ALIVE_NO_WRITERS_INSTANCE_STATE) {
					logger.debug(
							"Built-in Reader: publisher was deleted:" + 
					        "\n\tinstance_handle->"+
						    info.instance_handle.toString());
					delete_publisher();
				}

			}
		} catch (RETCODE_NO_DATA noData) {
			return;
		} catch (Exception e) {
		}

	}
	private void add_publisher(){
		String userData =
				new String(publication_builtin_topic_data.user_data.value.toArrayByte(null));

		//Process only if this publisher is a client publisher in our local domain 
		if (!(userData.equals(TOPIC_ROUTE_STRING_CODE))) {
			//Cache topic name of discovered Publisher
			String topic=publication_builtin_topic_data.topic_name.replaceAll("\\s", "");

			//Add this instance handle to topic mapping to instanceHandle_topic map
			instanceHandle_topic_map.put(info.instance_handle.toString(), topic);

			//Update the current count of publishers for this topic in the domain
			if (topic_publisherCount_map.containsKey(topic)) {
				topic_publisherCount_map.put(topic, topic_publisherCount_map.get(topic) + 1);
			} else {
				topic_publisherCount_map.put(topic, 1);
			}

			logger.debug(String.format("Current publisher count for topic:%s is %d\n",
					topic,topic_publisherCount_map.get(topic)));


			//Update the current publisher count for topic t at ZK path /topics/t/pub/ebLocator
			if (topic_publisherCount_map.get(topic)==1){
				create_EB_znode(topic,publication_builtin_topic_data);

				//Create topic session for topic t if it does not already exist
				logger.debug(String.format("Creating topic session for topic:%s\n",topic));
				create_topic_session(publication_builtin_topic_data.topic_name,
						publication_builtin_topic_data.type_name);

				//Install listener for RB assignment for topic t
				install_topic_to_rb_assignment_listener(topic);
			}

		} else {
			logger.debug("This publisher was created by RS\n");
		}
	}

	private void delete_publisher(){
		//retrieve the topic name for which this publisher was removed
		String topic=instanceHandle_topic_map.getOrDefault(info.instance_handle.toString(),null);
		if (topic==null){
			logger.debug("This publisher was created by RS\n");
			return;
		}
		//remove the instance handle 
		instanceHandle_topic_map.remove(info.instance_handle.toString());

		//update the current count of publishers for this topic
		int updated_count=topic_publisherCount_map.get(topic)-1;

		logger.debug(String.format("Current count for topic:%s is %d\n",
   			 topic,updated_count));

		if (updated_count==0){
			topic_publisherCount_map.remove(topic);
		}else{
			topic_publisherCount_map.put(topic, updated_count);
		}
		
		//Remove EB znode under /topics/t/pub if publisher count in this domain is 0
		if (updated_count==0){
			PublicationBuiltinTopicData publication_builtin_topic_data=
					delete_EB_znode(topic);

			//Remove topic session if publisher count==0
			logger.debug(String.format("Removing session route for %s as publisher count is 0\n", topic));
			rs.deleteTopicSession(String.format("%s::%sSubscriptionSession",
					localDomainRouteName,publication_builtin_topic_data.topic_name));
			rs.deleteTopicSession(String.format("%s::%sSubscriptionSession",
					pubDomainRouteName,publication_builtin_topic_data.topic_name));

			//Remove listener for RB assignment if publisher count=0
			NodeCache topicCache=topic_topicCache_map.remove(topic);
			String RB_address= new String(topicCache.getCurrentData().getData());
			String topic_path= topicCache.getCurrentData().getPath();
			logger.debug(String.format("Removing listener for RB assignment for topic node:%s as publisher count is 0\n",
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
   	 				/*remove RB as peer 
   	 				logger.debug(String.format("Local domain is not connected to RB:%s for any topics.\n"
   	 						+ "Hence, removing RB:%s as peer.\n",RB_address,RB_address));
   	 				String rbLocator = "tcpv4_wan://" + RB_address + ":" + RB_P1_BIND_PORT;
   	 				rs.removePeer(domainRouteName,rbLocator,false);
   	 				rb_topics_map.remove(RB_address);*/
   	 			}
			}
		}
   	 		
	}
	private void create_EB_znode(String topic,PublicationBuiltinTopicData publication_builtin_data){
		//ensure topic path /topics/t/pub and /topics/t/sub exists
		ensure_topic_path_exists(topic);
		String parent_path= (CuratorHelper.TOPIC_PATH+"/"+topic+"/pub");
		String znode_name=ebAddress;
		String path=ZKPaths.makePath(parent_path, znode_name);
		try {
			client.create().
				creatingParentsIfNeeded().
				withMode(CreateMode.PERSISTENT).
				forPath(path, CuratorHelper.serialize(publication_builtin_data));
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		logger.debug(String.format("Created EB znode for EB:%s under topic path:%s\n",
				ebAddress,parent_path));
	}
	
	private PublicationBuiltinTopicData delete_EB_znode(String topic){
		PublicationBuiltinTopicData publication_builtin_topic_data=null;
		String parent_path= (CuratorHelper.TOPIC_PATH+"/"+topic+"/pub");
		String znode_name=ebAddress;
		String path=ZKPaths.makePath(parent_path, znode_name);
		try {
			publication_builtin_topic_data=(PublicationBuiltinTopicData) CuratorHelper.
					deserialize(client.getData().forPath(path));
			client.delete().forPath(path);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		logger.debug(String.format("Deleted EB znode:%s under topic path:%s "
				+ "as publisher count for topic:%s is 0\n",
				ebAddress,parent_path,topic));
		return publication_builtin_topic_data;
	}
	
	private void ensure_topic_path_exists(String topic){
		String topic_subscribers_path=CuratorHelper.TOPIC_PATH+"/"+topic+"/sub";
		String topic_publishers_path=CuratorHelper.TOPIC_PATH+"/"+topic+"/pub";
		try {
			if(client.checkExists().forPath(topic_subscribers_path)==null)
			{
				client.create().
					creatingParentsIfNeeded().
					forPath(topic_subscribers_path);
				logger.debug(String.format("EB:%s created topic path for subscribers:%s\n",
						ebAddress,topic_subscribers_path));
			}
			if(client.checkExists().forPath(topic_publishers_path)==null)
			{
				client.create().
					creatingParentsIfNeeded().
					forPath(topic_publishers_path);
				logger.debug(String.format("EB:%s created topic path for publishers:%s\n",
						ebAddress,topic_publishers_path));
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}	
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
							 String rbLocator = "tcpv4_wan://" + rb_address + ":" + RB_P1_BIND_PORT;
							 logger.debug(String.format("Adding RB:%s as peer\n",rbLocator));
							 rs.addPeer(pubDomainRouteName,rbLocator, false);
						 }else{
							 HashSet<String> topics=rb_topics_map.get(rb_address);
							 topics.add(topic_path);
							 logger.debug(String.format("RB:%s for topic:%s already exists as peer\n",
									 rb_address,topic_path));
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
	
	private void create_topic_session(String topic_name,String type_name){
		String command_string="str://\"<session name=\"" + topic_name + "SubscriptionSession\">" +
                         "<topic_route name=\"" + topic_name + "SubscriptionRoute\">" +
                         "<route_types>true</route_types>" +
                         "<publish_with_original_info>true</publish_with_original_info>" +
                         "<publish_with_original_timestamp>true</publish_with_original_timestamp>" +
                         "<input participant=\"1\">" +
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
                         "</session>\"";

		//Create topic session in localDomainRoute
		rs.createTopicSession(localDomainRouteName, command_string);
		//Create topic session in pubDomainRoute
		rs.createTopicSession(pubDomainRouteName, command_string);
	}

}
