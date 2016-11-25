package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.net.*;
import java.util.*;
import com.rti.idl.RTI.RoutingService.Administration.CommandKind;
import com.rti.dds.publication.builtin.*;
import com.rti.dds.subscription.builtin.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
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

    private static final String DOMAIN_ROUTE_NAME_PREFIX = "RoutingBrokerDomainRoute";
    private static final String TOPIC_ROUTE_CODE = "107"; //this means letter 'k'

    private RoutingServiceAdministrator rs= null;
    private String rbAddress;
    private String domainRouteName;

    private String zkConnector= "localhost:2181";
    private CuratorFramework client = null;

    // Curator node cache for this routing broker's znode under /routingBroker
    private NodeCache rbNodeCache = null;
    // Curator path children cache for publishers of an assigned topic t under /topics/t/pub
    private HashMap<String, PathChildrenCache> publishersForTopic = new HashMap<String, PathChildrenCache>();
    // Curator path children cache for subscribers of an assigned topic t under /topics/t/sub
    private HashMap<String, PathChildrenCache> subscribersForTopic = new HashMap<String, PathChildrenCache>();

    // List of Edge Broker locators connected to RB_P1_BIND_PORT
    private HashSet<String> p1PeerList = new HashSet<String>();
    // List of Edge Broker locators connected to RB_P2_BIND_PORT
    private HashSet<String> p2PeerList = new HashSet<String>();
    
    // Used for managing topics assigned to this routing broker
    private HashSet<String> subTopicList = new HashSet<String>();
    private HashSet<String> pubTopicList = new HashSet<String>();
    private HashSet<String> topicList = new HashSet<String>();

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

    	logger.debug(String.format("Starting Routing Broker at:%s  with ZK Connector:%s\n",
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
        	// Ensure ZK paths /routingBroker and /leader exist
        	try {
        		client.create().withMode(CreateMode.PERSISTENT).forPath(CuratorHelper.ROUTING_BROKER_PATH, new byte[0]);
        		client.create().withMode(CreateMode.PERSISTENT).forPath(CuratorHelper.LEADER_PATH, new byte[0]);
        	} catch (KeeperException.NodeExistsException e) {
        		System.out.println("Exception:" + e.getMessage());
        	}
        	// Create a NodeCache for this routing broker
            rbNodeCache = new NodeCache(client, CuratorHelper.ROUTING_BROKER_PATH + "/" + rbAddress);
            rbNodeCache.start();
            // Install listner on this RB NodeCache to listen for topic assignments
            addRbNodeListener(rbNodeCache);

            // Create znode with ephemeral mode for this routing broker
            HashSet<String> topicSet = new HashSet<String>();
            client.create().withMode(CreateMode.EPHEMERAL).
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
                    if (!publishersForTopic.containsKey(topic)) {
                    	logger.debug(String.format("RB:%s was assigned new topic:%s.\n "
                    			+ "Installing listeners to be notified when publishers for topic:%s join\n",rbAddress,topic,topic));
                        String publishersForTopicPath= topic + "/pub";
                        PathChildrenCache topicPubCache = new PathChildrenCache(client, publishersForTopicPath, true);
                        topicPubCache.start();
                        publishersForTopic.put(topic, topicPubCache);
                        addPubChildrenListener(topicPubCache);
                    }
                    if (!subscribersForTopic.containsKey(topic)) {
                    	logger.debug(String.format("RB:%s was assigned new topic:%s.\n "
                    			+ "Installing listeners to be notified when subscribers for topic:%s join\n",rbAddress,topic,topic));
                        String subscribersForTopicPath = topic + "/sub";
                        PathChildrenCache topicSubCache = new PathChildrenCache(client, subscribersForTopicPath, true);
                        topicSubCache.start();
                        subscribersForTopic.put(topic, topicSubCache);
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
                    //When a new publisher for topic joins 
                    case CHILD_ADDED: {
                        // Deserialize publication built-in topic data
                        PublicationBuiltinTopicData pubBuiltinTopicData =
                                (PublicationBuiltinTopicData) CuratorHelper.deserialize(event.getData().getData());
                        String ebLocator = "tcpv4_wan://" + new String(pubBuiltinTopicData.user_data.value.toArrayByte(null));
                        String topicName = pubBuiltinTopicData.topic_name;
                        
                        logger.debug(String.format("New Publisher with ebLocator:%s joined for topic:%s\n", 
                        		ebLocator,topicName));

                        // If edge broker's locator is not added in peer list, needs to be added
                        // call addPeer function to add this peer into Routing Service
                        if (!p1PeerList.contains(ebLocator)) {
                            p1PeerList.add(ebLocator);
                            logger.debug(String.format("Adding ebLocator:%s for new publisher for topic:%s to RB_P1_BIND_PORT PeerList\n", 
                        		ebLocator,topicName));
                            rs.addPeer(domainRouteName,ebLocator, true);
                        }

                        // if publication topic route is not created, needs to be created
                        if (!pubTopicList.contains(topicName)) {
                            pubTopicList.add(topicName);
                            // topic route is created only when both subscription and publication exist
                            if (subTopicList.contains(topicName) && pubTopicList.contains(topicName))
                                topicList.add(topicName);
                            if (topicList.contains(topicName)){
                            	logger.debug(String.format("Both publishers and subscribers for topic:%s exist,"
                            			+ " creating TopicRoute for topic:%s\n",topicName,topicName));
                                createTopicRoute(pubBuiltinTopicData.topic_name,pubBuiltinTopicData.type_name);
                            }
                        }
                        break;
                    }
                    case CHILD_REMOVED: {
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
                    // When a new subscriber for topic joins
                    case CHILD_ADDED: {

                        // Deserialize subscription built-in topic data
                        SubscriptionBuiltinTopicData subBuiltinTopicData =
                                (SubscriptionBuiltinTopicData) CuratorHelper.deserialize(event.getData().getData());
                        String ebLocator = "tcpv4_wan://" + new String(subBuiltinTopicData.user_data.value.toArrayByte(null));

                        String topicName = subBuiltinTopicData.topic_name;
                        
                        logger.debug(String.format("New Subscriber with ebLocator:%s joined for topic:%s\n", 
                        		ebLocator,topicName));

                        // If edge broker's locator is not added in peer list, needs to be added
                        // call addPeer function to add this peer into Routing Service
                        if (!p2PeerList.contains(ebLocator)) {
                            p2PeerList.add(ebLocator);
                            logger.debug(String.format("Adding ebLocator:%s for new subscriber for topic:%s to RB_P2_BIND_PORT PeerList\n", 
                            		ebLocator,topicName));
                            rs.addPeer(domainRouteName,ebLocator, false);
                        }

                        // if subscription topic route is not created, needs to be created
                        if (!subTopicList.contains(topicName)) {
                            subTopicList.add(topicName);
                            // topic route is created only when both subscription and publication exist
                            if (subTopicList.contains(topicName) && pubTopicList.contains(topicName))
                                topicList.add(topicName);
                            if (topicList.contains(topicName)){
                            	logger.debug(String.format("Both publishers and subscribers for topic:%s exist,"
                            			+ " creating TopicRoute for topic:%s\n",topicName,topicName));
                                createTopicRoute(subBuiltinTopicData.topic_name,subBuiltinTopicData.type_name);
                            }
                        }

                        break;
                    }
                    case CHILD_REMOVED: {
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

    	rs.sendRequest(CommandKind.RTI_ROUTING_SERVICE_COMMAND_CREATE, 
    			 "str://\"<domain_route name=\"" + domainRouteName + "\">" +
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
    private void createTopicRoute(String topic_name,String type_name) {
    	logger.debug(String.format("Creating TopicRoute for topic:%s\n",topic_name ));

    	rs.sendRequest(CommandKind.RTI_ROUTING_SERVICE_COMMAND_CREATE, 
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
