package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.net.*;
import java.util.*;
import org.apache.zookeeper.CreateMode;

import com.rti.dds.publication.builtin.PublicationBuiltinTopicData;
import com.rti.dds.subscription.builtin.SubscriptionBuiltinTopicData;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RoutingBroker {
	private Logger logger;
	// Domain Id in which Routing Brokers operate
    private static final int WAN_DOMAIN_ID = 230;
    // Public facing ports for interconnection of domains
    private static final String RB_P1_BIND_PORT = "8500"; //only for receiving data 
    private static final String RB_P2_BIND_PORT = "8501"; //only for sending data 
    //public facing port of local domains
    private static final String EB_P2_BIND_PORT = "8502"; 

    private static final String DOMAIN_ROUTE_NAME_PREFIX = "RoutingBrokerDomainRoute";
    private static final String TOPIC_ROUTE_CODE = "107"; //this means letter 'k'

    private RoutingServiceAdministrator rs= null;
    private String rbAddress;
    private String domainRouteName;

    private String zkConnector;
    private CuratorFramework client = null;

    // Curator node cache for this routing broker's znode under /routingBroker
    private NodeCache rbNodeCache = null;

    // Curator path children cache for publishers of an assigned topic t under /topics/t/pub
    private HashMap<String, PathChildrenCache> topic_publishersChildrenCache_map = new HashMap<String, PathChildrenCache>();
    // Curator path children cache for subscribers of an assigned topic t under /topics/t/sub
    private HashMap<String, PathChildrenCache> topic_subChildrenCache_map = new HashMap<String, PathChildrenCache>();

    // Map for keeping track of active topics in EB domains interfacing with RB_P1_BIND_PORT
    private HashMap<String,HashSet<String>> p1_eb_topics_map=new HashMap<>();
    // Map for keeping track of active topics in EB domains interfacing with RB_P2_BIND_PORT
    private HashMap<String,HashSet<String>> p2_eb_topics_map=new HashMap<>();
    
    // Used for managing topics assigned to this routing broker
    private HashSet<String> subscribedTopics = new HashSet<String>();
    private HashSet<String> publishedTopics = new HashSet<String>();
    private HashSet<String> topicRoutesList = new HashSet<String>();

    public RoutingBroker(String zkConnector) {
    	//configure logger
    	logger= LogManager.getLogger(this.getClass().getSimpleName());
    	// ZK server address
    	this.zkConnector=zkConnector;
    	
        try {
            rbAddress= InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
            logger.error(e.getMessage(),e);
        }

    	logger.debug(String.format("Starting Routing Broker at:%s with ZK Connector:%s\n",
    			rbAddress,zkConnector));

        // Create Routing Service remote administrator 
        try {
			rs=new RoutingServiceAdministrator(rbAddress);
		} catch (Exception e) {
            logger.error(e.getMessage(),e);
		}
    }

    public static void main(String args[]){
    	if (args.length <1){
    		System.out.println("Enter zk connector string: address:port");
    		return;
    	}
    	PropertyConfigurator.configure("log4j.properties");
    	String zkConnector=args[0];
    	new RoutingBroker(zkConnector).start();
    }

    public void start(){
    	// Create domain route between RB_P1_BIND_PORT and RB_P2_BIND_PORT
    	domainRouteName=DOMAIN_ROUTE_NAME_PREFIX + "@"+ rbAddress;
    	createDomainRoute();

    	// Connect to ZK
    	client = CuratorFrameworkFactory.newClient(zkConnector,
                new ExponentialBackoffRetry(1000, 3));
        client.start();
        
        try{
        	// Ensure ZK paths /routingBroker /topics and /leader exists
        	if(client.checkExists().forPath(CuratorHelper.ROUTING_BROKER_PATH) == null){
        		logger.debug(String.format("zk path:%s does not exist. RB:%s will create zk path:%s\n",
        				CuratorHelper.ROUTING_BROKER_PATH,rbAddress,CuratorHelper.ROUTING_BROKER_PATH));
        		client.create().
        			withMode(CreateMode.PERSISTENT).
        			forPath(CuratorHelper.ROUTING_BROKER_PATH, new byte[0]);
        	}
        	if(client.checkExists().forPath(CuratorHelper.LEADER_PATH)== null){
        		logger.debug(String.format("zk path:%s does not exist. RB:%s will create zk path:%s\n",
        				CuratorHelper.LEADER_PATH,rbAddress,CuratorHelper.LEADER_PATH));
        		client.create().
        			withMode(CreateMode.PERSISTENT).
        			forPath(CuratorHelper.LEADER_PATH, new byte[0]);
        	}
        	if(client.checkExists().forPath(CuratorHelper.TOPIC_PATH)== null){
        		logger.debug(String.format("zk path:%s does not exist. RB:%s will create zk path:%s\n",
        				CuratorHelper.TOPIC_PATH,rbAddress,CuratorHelper.TOPIC_PATH));
        		client.create().
        			withMode(CreateMode.PERSISTENT).
        			forPath(CuratorHelper.TOPIC_PATH, new byte[0]);
        	}
        	// Create a NodeCache for this routing broker
            rbNodeCache = new NodeCache(client, CuratorHelper.ROUTING_BROKER_PATH + "/" + rbAddress);
            rbNodeCache.start();
            // Install listner on this RB NodeCache to listen for topic assignments
            addRbNodeListener(rbNodeCache);

            // Create znode with ephemeral mode for this routing broker
            logger.debug(String.format("Creating znode for this routing broker:%s\n", rbAddress));
            HashSet<String> topicSet = new HashSet<String>();
            client.create().
            	withMode(CreateMode.EPHEMERAL).
            	forPath(ZKPaths.makePath(CuratorHelper.ROUTING_BROKER_PATH, rbAddress), CuratorHelper.serialize(topicSet));

            // Create a thread for leader election and some work done by a leader
            new Thread(new LeaderThread(client, rbAddress)).start();

            while (true) {
                Thread.sleep(1000);
            }
        }catch(Exception e){
            logger.error(e.getMessage(),e);
        }finally{
        	CloseableUtils.closeQuietly(rbNodeCache);
            CloseableUtils.closeQuietly(client);        	
        }

    }
    
    private void addRbNodeListener(final NodeCache cache) {
        cache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                @SuppressWarnings("unchecked")
                //Obtain updated set of topics assigned to this routing broker
				HashSet<String> topicSet = (HashSet<String>) CuratorHelper.
					deserialize(cache.getCurrentData().getData());
                
                logger.debug(String.format("Number of topics assigned to RB:%s is %d\n",
                		rbAddress,topicSet.size()));

                for (String topic : topicSet) {
                	// For a new topic t, register path children listeners for /topics/t/pub and /topics/t/sub
                    if (!topic_publishersChildrenCache_map.containsKey(topic)) {
                    	logger.debug(String.format("RB:%s was assigned new topic:%s.\n "
                    			+ "Installing listeners to be notified when publishers for topic:%s join\n",rbAddress,topic,topic));
                        String publishersForTopicPath= CuratorHelper.TOPIC_PATH+ "/" + topic + "/pub";
                        PathChildrenCache topicPubCache = new PathChildrenCache(client, publishersForTopicPath, true);
                        topicPubCache.start();
                        topic_publishersChildrenCache_map.put(topic, topicPubCache);
                        addPubChildrenListener(topicPubCache);
                    }
                    if (!topic_subChildrenCache_map.containsKey(topic)) {
                    	logger.debug(String.format("RB:%s was assigned new topic:%s.\n "
                    			+ "Installing listeners to be notified when subscribers for topic:%s join\n",rbAddress,topic,topic));
                        String subscribersForTopicPath = CuratorHelper.TOPIC_PATH+ "/" + topic + "/sub";
                        PathChildrenCache topicSubCache = new PathChildrenCache(client, subscribersForTopicPath, true);
                        topicSubCache.start();
                        topic_subChildrenCache_map.put(topic, topicSubCache);
                        addSubChildrenListener(topicSubCache);
                    }
                }
            }
        });
    }
    
    private void addPubChildrenListener(PathChildrenCache cache) {
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    //When a new domain with publishers for topic t joins 
                    case CHILD_ADDED: {
                    	String eb_path=event.getData().getPath();
                    	String topic= eb_path.split("/")[2];
                    	PublicationBuiltinTopicData publication_builtin_data=
                    			(PublicationBuiltinTopicData)CuratorHelper.deserialize(event.getData().getData());
                    	String eb_address= eb_path.split("/")[4];
                    	String eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_BIND_PORT;

                    	logger.debug(String.format("Publishers for topic:%s discovered in EB:%s domain\n",
                    			topic,eb_address));
                    	
                    	if(!p1_eb_topics_map.containsKey(eb_address)){
                    		p1_eb_topics_map.put(eb_address, new HashSet<String>());
                    		p1_eb_topics_map.get(eb_address).add(topic);
                    		logger.debug(String.format("Adding eb:%s as peer for RB_P1_BIND_PORT:%s\n",
                    				eb_locator,RB_P1_BIND_PORT));
                    		rs.addPeer(domainRouteName, eb_locator, true);
                    		
                    	}else{
                    		logger.debug(String.format("eb:%s already exists as peer for RB_P1_BIND_PORT:%s\n", 
                    				eb_locator,RB_P1_BIND_PORT));
                    		p1_eb_topics_map.get(eb_address).add(topic);
                    	}

                        // if topic route is not created, it needs to be created
                        if (!publishedTopics.contains(topic)) {
                            publishedTopics.add(topic);
                            //check if subscribers for this topic exist
                            if(subscribedTopics.contains(topic)){
                            	//Create topic route only if it does not already exist
                            	if(!topicRoutesList.contains(topic)){
                            		logger.debug(String.format("Both publishers and subscribers for topic:%s exist,"
                            				+ " creating Topic Session for topic:%s\n",topic,topic));
                            		createTopicSession(publication_builtin_data.topic_name,
                            				publication_builtin_data.type_name);
                            		topicRoutesList.add(topic);
                            	}
                            }
                        }
                        break;
                    }
                    case CHILD_REMOVED: {
                    	String eb_path=event.getData().getPath();
                    	String topic= eb_path.split("/")[2];
                    	logger.debug(String.format("EB:%s was removed\n", eb_path));
                    	
                    	PublicationBuiltinTopicData publication_builtin_topic_data= 
                    			(PublicationBuiltinTopicData) CuratorHelper.deserialize(event.getData().getData());

                    	PathChildrenCache topic_pub_children_cache= topic_publishersChildrenCache_map.get(topic);
                    	topic_pub_children_cache.rebuild();

                    	if(topic_pub_children_cache.getCurrentData().isEmpty()){
                    		logger.debug(String.format("There are no publishing domains for topic:%s\n", topic));
                    		publishedTopics.remove(topic);
                    		if(topicRoutesList.contains(topic)){
                    			topicRoutesList.remove(topic);
                    			logger.debug(String.format("Publishing domains for topic:%s do not exist.\n"
                    					+ "Removing topic session:%s\n",topic,String.format("%s::%sTopicSession",
                    							domainRouteName,publication_builtin_topic_data.topic_name)));
                    			rs.deleteTopicSession(String.format("%s::%sTopicSession",
                    					domainRouteName,publication_builtin_topic_data.topic_name) );
                    		}
                    	}
                    	
                        break;
                    }
				default:
					break;
                }
            }
        });
    }
    
    private void addSubChildrenListener(PathChildrenCache cache) {
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    // When a local domain with subscribers for topic t is discovered 
                    case CHILD_ADDED: {
                    	String eb_path=event.getData().getPath();
                    	String topic= eb_path.split("/")[2];
                    	SubscriptionBuiltinTopicData subscription_builtin_data=
                    			(SubscriptionBuiltinTopicData)(CuratorHelper.deserialize(event.getData().getData()));
                    	String eb_address= eb_path.split("/")[4];
                    	String eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_BIND_PORT;

                    	logger.debug(String.format("Subscriber for topic:%s discovered in EB:%s domain\n",
                    			topic,eb_address));
                    	
                    	if(!p2_eb_topics_map.containsKey(eb_address)){
                    		p2_eb_topics_map.put(eb_address, new HashSet<String>());
                    		p2_eb_topics_map.get(eb_address).add(topic);
                    		logger.debug(String.format("Adding eb:%s as peer for RB_P2_BIND_PORT:%s\n",
                    				eb_locator,RB_P2_BIND_PORT));
                    		rs.addPeer(domainRouteName, eb_locator, false);
                    		
                    	}else{
                    		logger.debug(String.format("eb:%s already exists as peer for RB_P2_BIND_PORT:%s\n", 
                    				eb_locator,RB_P2_BIND_PORT));
                    		p1_eb_topics_map.get(eb_address).add(topic);
                    	}

                        // if topic route is not created, it needs to be created
                        if (!subscribedTopics.contains(topic)) {
                            subscribedTopics.add(topic);
                            //check if publishers for this topic exist
                            if(publishedTopics.contains(topic)){
                            	//Create topic route only if it does not already exist
                            	if(!topicRoutesList.contains(topic)){
                            		logger.debug(String.format("Both publishers and subscribers for topic:%s exist,"
                            				+ " creating Topic Session for topic:%s\n",topic,topic));
                            		createTopicSession(subscription_builtin_data.topic_name,
                            				subscription_builtin_data.type_name);
                            		topicRoutesList.add(topic);
                            	}
                            }
                        }
                        break;
                    }
                    case CHILD_REMOVED: {
                    	String eb_path=event.getData().getPath();
                    	String topic= eb_path.split("/")[2];

                    	logger.debug(String.format("EB:%s was removed\n", eb_path));
                    	
                    	SubscriptionBuiltinTopicData subscription_builtin_topic_data=
                    			(SubscriptionBuiltinTopicData) CuratorHelper.deserialize(event.getData().getData());

                    	PathChildrenCache topic_sub_children_cache= topic_subChildrenCache_map.get(topic);
                    	topic_sub_children_cache.rebuild();

                    	if(topic_sub_children_cache.getCurrentData().isEmpty()){
                    		logger.debug(String.format("There are no subscribing domains for topic:%s\n", topic));
                    		subscribedTopics.remove(topic);
                    		if(topicRoutesList.contains(topic)){
                    			topicRoutesList.remove(topic);
                    			logger.debug(String.format("Subscribing domains for topic:%s do not exist.\n"
                    					+ "Removing topic session:%s\n",topic,String.format("%s::%sTopicSession",
                    							domainRouteName,subscription_builtin_topic_data.topic_name)));
                    			rs.deleteTopicSession(String.format("%s::%sTopicSession",
                    					domainRouteName,subscription_builtin_topic_data.topic_name) );
                    		}
                    	}
                        break;
                    }
				default:
					break;
                }
            }
        });
    }
    
    private void createDomainRoute(){
    	logger.debug(String.format("Creating domain route:%s for interconnecting domains between:%s and %s",
    			domainRouteName,RB_P1_BIND_PORT,RB_P2_BIND_PORT));

    	rs.createDomainRoute("str://\"<domain_route name=\"" + domainRouteName + "\">" +
                         "<entity_monitoring>" +
                         "<historical_statistics><up_time>true</up_time></historical_statistics>" +
                         "</entity_monitoring>" +
                         "<participant_1>" +
                         "<domain_id>" + WAN_DOMAIN_ID + "</domain_id>" +
                         "<participant_qos>" +
                         "<transport_builtin><mask>MASK_NONE</mask></transport_builtin>" +
                         "<property><value>" +
                         "<element><name>dds.transport.load_plugins</name><value>dds.transport.TCPv4.tcp1</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.library</name><value>nddstransporttcp</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.create_function</name><value>NDDS_Transport_TCPv4_create</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.parent.classid</name><value>NDDS_TRANSPORT_CLASSID_TCPV4_WAN</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.public_address</name><value>" +
                         rbAddress + ":" + RB_P1_BIND_PORT +
                         "</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.server_bind_port</name><value>" +
                         RB_P1_BIND_PORT + "</value></element>" +
                         "</value></property>" +
                         "</participant_qos>" +
                         "</participant_1>" +
                         "<participant_2>" +
                         "<domain_id>" + WAN_DOMAIN_ID + "</domain_id>" +
                         "<participant_qos>" +
                         "<transport_builtin><mask>MASK_NONE</mask></transport_builtin>" +
                         "<property><value>" +
                         "<element><name>dds.transport.load_plugins</name><value>dds.transport.TCPv4.tcp1</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.library</name><value>nddstransporttcp</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.create_function</name><value>NDDS_Transport_TCPv4_create</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.parent.classid</name><value>NDDS_TRANSPORT_CLASSID_TCPV4_WAN</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.public_address</name><value>" +
                         rbAddress + ":" + RB_P2_BIND_PORT +
                         "</value></element>" +
                         "<element><name>dds.transport.TCPv4.tcp1.server_bind_port</name><value>" +
                         RB_P2_BIND_PORT +
                         "</value></element>" +
                         "</value></property>" +
                         "</participant_qos>" +
                         "</participant_2>" +
                         "</domain_route>\"");
    }
    
    private void createTopicSession(String topic_name,String type_name) {
    	logger.debug(String.format("Creating Topic Session for topic:%s\n",topic_name ));

    	rs.createTopicSession(domainRouteName,
    			  "str://\"<session name=\"" + topic_name + "TopicSession\">" +
                          "<topic_route name=\"" + topic_name + "TopicRoute\">" +
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
                          "</session>\"");
    }


}
