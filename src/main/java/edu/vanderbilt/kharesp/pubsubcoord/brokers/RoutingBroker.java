package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.net.*;
import java.util.*;
import org.apache.zookeeper.CreateMode;
import com.rti.dds.publication.builtin.PublicationBuiltinTopicData;
import com.rti.dds.subscription.builtin.SubscriptionBuiltinTopicData;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import edu.vanderbilt.kharesp.pubsubcoord.routing.DomainRoute;
import edu.vanderbilt.kharesp.pubsubcoord.routing.RoutingServiceAdministrator;
import edu.vanderbilt.kharesp.pubsubcoord.routing.TopicSession;

public class RoutingBroker {
	private Logger logger;
	// Domain Id in which Routing Brokers operate
    public static final int WAN_DOMAIN_ID = 230;
    // Public facing ports for interconnection of domains
    public static final String RB_P1_BIND_PORT = "8500"; //only for receiving data 
    public static final String RB_P2_BIND_PORT = "8501"; //only for sending data 

    //public facing port of local domains
    public static final String EB_P2_BIND_PORT = "8502"; 
    public static final String EB_P2_PUB_BIND_PORT="8502";
    public static final String EB_P2_SUB_BIND_PORT="8503";

    private static final String DOMAIN_ROUTE_NAME_PREFIX = "RoutingBrokerDomainRoute";

    private String rbAddress;
    private String domainRouteName;
    private boolean emulated_broker;
    private RoutingServiceAdministrator rs= null;

    private CuratorHelper client = null;
    // Curator node cache for this routing broker's znode under /routingBroker
    private NodeCache rbNodeCache = null;

    // Curator path childrencache for a published topic t under /topics/t/pub
    private HashMap<String, PathChildrenCache> publishedTopic_childrenCache_map = new HashMap<String, PathChildrenCache>();
    // Curator path Childrencache for a subscribted topic t under /topics/t/sub
    private HashMap<String, PathChildrenCache> subscribedTopic_childrenCache_map = new HashMap<String, PathChildrenCache>();

    // Map for keeping track of active topics in EB domains interfacing with RB_P1_BIND_PORT
    private HashMap<String,HashSet<String>> region_publishedTopics_map=new HashMap<>();
    // Map for keeping track of active topics in EB domains interfacing with RB_P2_BIND_PORT
    private HashMap<String,HashSet<String>> region_subscribedTopics_map=new HashMap<>();
    
    //Map for keeping track of subscribing regions for a topic 
    private HashMap<String,Set<String>> topic_subscribingRegions = new HashMap<>();
    //Map for keeping track of publishing regions for a topic 
    private HashMap<String,Set<String>> topic_publishingRegions= new HashMap<>();
    
    // Used for managing topics assigned to this routing broker
    private HashSet<String> subscribedTopics = new HashSet<String>();
    private HashSet<String> publishedTopics = new HashSet<String>();
    private HashSet<String> topicRoutesList = new HashSet<String>();

    public RoutingBroker(String zkConnector,boolean emulated_broker) {
    	//configure logger
    	logger= LogManager.getLogger(this.getClass().getSimpleName());

        try {
            rbAddress= InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
            logger.error(e.getMessage(),e);
        }
    	this.emulated_broker=emulated_broker;
    	
    	// Connect to ZK
    	client = new CuratorHelper(zkConnector);

        // Create Routing Service remote administrator 
        try {
			rs=new RoutingServiceAdministrator();
		} catch (Exception e) {
            logger.error(e.getMessage(),e);
		}

    	logger.debug(String.format("Starting Routing Broker at:%s with ZK Connector:%s\n",
    			rbAddress,zkConnector));
    }

    public static void main(String args[]){
    	if (args.length <2){
    		System.out.println("Usage:  zk_address:zk_port, emulated_broker(0/1)");
    		return;
    	}
    	PropertyConfigurator.configure("log4j.properties");
    	String zkConnector=args[0];
    	int emulated_broker=Integer.parseInt(args[1]);
    	new RoutingBroker(zkConnector,emulated_broker>0?true:false).start();
    }

    public void start(){
    	// Create domain route between RB_P1_BIND_PORT and RB_P2_BIND_PORT
    	domainRouteName=DOMAIN_ROUTE_NAME_PREFIX + "@"+ rbAddress;
    	logger.debug(String.format("Creating domain route:%s for interconnecting domains between:%s and %s",
    			domainRouteName,RB_P1_BIND_PORT,RB_P2_BIND_PORT));
    	rs.createDomainRoute(domainRouteName, DomainRoute.RB_DOMAIN_ROUTE);
        
		// Ensure ZK paths /routingBroker /topics and /leader exists
		client.ensurePathExists(CuratorHelper.ROUTING_BROKER_PATH);
		client.ensurePathExists(CuratorHelper.LEADER_PATH);
		client.ensurePathExists(CuratorHelper.TOPIC_PATH);

        try{
        	// Create a NodeCache for this routing broker
        	String rbPath= CuratorHelper.ROUTING_BROKER_PATH + "/" + rbAddress;
            rbNodeCache = client.nodeCache(rbPath);
            rbNodeCache.start();
            // Install listner on this RB NodeCache to listen for topic assignments
            registerTopicAssignmentListener(rbNodeCache);

            // Create znode with ephemeral mode for this routing broker with an empty set of assigned topics
            logger.debug(String.format("Creating znode for this routing broker:%s\n", rbAddress));
            client.create(rbPath, new HashSet<String>(), CreateMode.EPHEMERAL);

            //Create leader thread
            new Thread(new LeaderThread(client, rbAddress)).start();

            while (true) {
                Thread.sleep(1000);
            }
        }catch(Exception e){
            logger.error(e.getMessage(),e);
        }finally{
        	CuratorHelper.closeQuitely(rbNodeCache);
        	client.close();
        }

    }
    
    private void registerTopicAssignmentListener(final NodeCache cache) {
        cache.getListenable().addListener(new NodeCacheListener() {

            @Override
            //called back when a topic is assigned to this RB
            public void nodeChanged() throws Exception {
                @SuppressWarnings("unchecked")
                //Obtain updated set of topics assigned to this routing broker
				HashSet<String> topicSet = (HashSet<String>) CuratorHelper.
					deserialize(cache.getCurrentData().getData());
                
                logger.debug(String.format("Number of topics assigned to RB:%s is %d\n",
                		rbAddress,topicSet.size()));

                for (String topic : topicSet) {
                	// For a new topic t, register a path children listener to listen for regions interested in topic t.
                	// Path Children listeners for paths: /topics/t/pub and /topics/t/sub
                    if (!publishedTopic_childrenCache_map.containsKey(topic)) {
                    	logger.debug(String.format("RB:%s was assigned new topic:%s.\n "
                    			+ "Installing listeners to be notified when publishers for topic:%s join\n",rbAddress,topic,topic));
                        String publishersForTopicPath= CuratorHelper.TOPIC_PATH+ "/" + topic + "/pub";

                        //create Path Children cache to listen for publishing regions for topic t under /topics/t/pub
                        PathChildrenCache topicPubCache = client.pathChildrenCache(publishersForTopicPath,true);
                        topicPubCache.start();
                        publishedTopic_childrenCache_map.put(topic, topicPubCache);

                        //register listener to respond to creation/deletion of publishing regions for topic t 
                        registerPublishingRegionListener(topicPubCache);
                    }
                    if (!subscribedTopic_childrenCache_map.containsKey(topic)) {
                    	logger.debug(String.format("RB:%s was assigned new topic:%s.\n "
                    			+ "Installing listeners to be notified when subscribers for topic:%s join\n",rbAddress,topic,topic));
                        String subscribersForTopicPath = CuratorHelper.TOPIC_PATH+ "/" + topic + "/sub";

                        //create Path Children cache to listen for subscribing regions for topic t under /topics/t/sub
                        PathChildrenCache topicSubCache = client.pathChildrenCache(subscribersForTopicPath,true);
                        topicSubCache.start();
                        subscribedTopic_childrenCache_map.put(topic, topicSubCache);
                        
                        //register listener to respond to creation/deletion of subscribing regions for topic t
                        registerSubscribingRegionListener(topicSubCache);
                    }
                }
            }
        });
    }
    
    private void registerPublishingRegionListener(PathChildrenCache cache) {
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    //When a new region with publishers for topic t joins 
                    case CHILD_ADDED: {
                    	String eb_path=event.getData().getPath();
                    	String topic= eb_path.split("/")[2];
                    	String eb_address= eb_path.split("/")[4];
                    	
                    	PublicationBuiltinTopicData publication_builtin_data=
                    			(PublicationBuiltinTopicData)CuratorHelper.deserialize(event.getData().getData());
                    	String eb_locator;
                    	if (emulated_broker){
                    		eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_PUB_BIND_PORT;
                    	}
                    	else{
                    		eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_BIND_PORT;
                    	}
                    	logger.debug(String.format("Publishers for topic:%s discovered in EB:%s domain\n",
                    			topic,eb_address));
                    	
                    	//Add this publishing region to topic t's set of publishing regions
                    	if(topic_publishingRegions.containsKey(topic)){
                    		topic_publishingRegions.get(topic).add(eb_address);
                    	}else{
                    		topic_publishingRegions.put(topic, new HashSet<String>(Arrays.asList(eb_address)));
                    	}
                    	
                    	
                    	//check if this region already exists as peer for our domain route
                    	if(!region_publishedTopics_map.containsKey(eb_address)){
                    		region_publishedTopics_map.put(eb_address, new HashSet<String>());
                    		region_publishedTopics_map.get(eb_address).add(topic);
                    		logger.debug(String.format("Adding eb:%s as peer for RB_P1_BIND_PORT:%s\n",
                    				eb_locator,RB_P1_BIND_PORT));
                    		//If this region does not exist as peer, add it as a peer for our domain route.
                    		rs.addPeer(domainRouteName, eb_locator, true);
                    		
                    	}else{
                    		logger.debug(String.format("eb:%s already exists as peer for RB_P1_BIND_PORT:%s\n", 
                    				eb_locator,RB_P1_BIND_PORT));
                    		//add this topic t, to the set of published topics in this region
                    		region_publishedTopics_map.get(eb_address).add(topic);
                    	}

                        // if topic route is not created, it needs to be created
                        if (!publishedTopics.contains(topic)) {
                            publishedTopics.add(topic);
                            //check if subscribers for this topic exist
                            if(subscribedTopics.contains(topic)){
                            	//If both publishers and subscribers for this topic exist, create topic route 
                            	if(!topicRoutesList.contains(topic)){
                            		Set<String> publishing_regions= topic_publishingRegions.get(topic);
                            		Set<String> subscribing_regions= topic_subscribingRegions.get(topic);
                            	
                            		//Determine whether to create a topic route or not
                            		if(publishing_regions.size()==1 && subscribing_regions.size()==1 &&
                            				publishing_regions.containsAll(subscribing_regions)){
                            			//topic route is not created when publishers and subscribers for topic t exist in the same region
                            			logger.debug(String.format("Publishers and Subscribers for topic:%s exist in the same region. "
                            					+ "Will not create topic session", topic));
                            			
                            		}else{
                            			// topic route is created only when publishers and subscribers for topic t exist in different regions
                            			logger.debug(String.format("Both publishers and subscribers for topic:%s exist,"
                            					+ " creating Topic Session for topic:%s\n", topic, topic));
                            			rs.createTopicSession(domainRouteName, publication_builtin_data.topic_name,
                            					publication_builtin_data.type_name, TopicSession.SUBSCRIPTION_SESSION);
                            			topicRoutesList.add(topic);
                            		}
                            	}
                            }
                        }
                        break;
                    }
                    //when a publishing region for topic t leaves
                    case CHILD_REMOVED: {
                    	String eb_path=event.getData().getPath();
                    	String topic= eb_path.split("/")[2];
                    	String eb_address= eb_path.split("/")[4];
                    	String eb_locator;
                    	if (emulated_broker){
                    		eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_PUB_BIND_PORT;
                    	}
                    	else{
                    		eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_BIND_PORT;
                    	}
                    	logger.debug(String.format("All Publishers for EB:%s domain have exited\n", eb_path));
                    	
                    	PublicationBuiltinTopicData publication_builtin_topic_data= 
                    			(PublicationBuiltinTopicData) CuratorHelper.deserialize(event.getData().getData());
                    	
                    	//remove this region from topic t's set of publishing regions
                    	if(topic_publishingRegions.containsKey(topic)){
                    		topic_publishingRegions.get(topic).remove(eb_address);
                    	}
                    	
                    	//remove this topic from p1_eb_topics_map
                    	if(region_publishedTopics_map.containsKey(eb_address)){
                    		region_publishedTopics_map.get(eb_address).remove(topic);

                    		//if there are no published topics for which we are interfacing with this region,remove this region as peer
                    		if(region_publishedTopics_map.get(eb_address).size()==0){
                    			rs.removePeer(domainRouteName, eb_locator, true);
                    			region_publishedTopics_map.remove(eb_address);
                    		}
                    	}

                    	//check if there are no publishing domains for topic t
                    	PathChildrenCache topic_pub_children_cache= publishedTopic_childrenCache_map.get(topic);
                    	topic_pub_children_cache.rebuild();

                    	if(topic_pub_children_cache.getCurrentData().isEmpty()){
                    		logger.debug(String.format("There are no publishing domains for topic:%s\n", topic));
                    		
                    		//remove topic t from list of published topics
                    		publishedTopics.remove(topic);
                    		if(topicRoutesList.contains(topic)){
                    			topicRoutesList.remove(topic);
                    			logger.debug(String.format("Publishing domains for topic:%s do not exist.\n"
                    					+ "Removing topic session:%s\n",topic,String.format("%s::%sTopicSession",
                    							domainRouteName,publication_builtin_topic_data.topic_name)));
                    			
                    			//Since there are no publishing regions for topic t, delete the topic session
                    			rs.deleteTopicSession(domainRouteName,publication_builtin_topic_data.topic_name,
                    					publication_builtin_topic_data.type_name,TopicSession.SUBSCRIPTION_SESSION);
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
    
    private void registerSubscribingRegionListener(PathChildrenCache cache) {
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    // When a new region with subscribers for topic t is discovered 
                    case CHILD_ADDED: {
                    	String eb_path=event.getData().getPath();
                    	String topic= eb_path.split("/")[2];
                    	String eb_address= eb_path.split("/")[4];
                    	SubscriptionBuiltinTopicData subscription_builtin_data=
                    			(SubscriptionBuiltinTopicData)(CuratorHelper.deserialize(event.getData().getData()));
                    	String eb_locator;
                    	if(emulated_broker){
                    		eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_SUB_BIND_PORT;
                    	}
                    	else{
                    		eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_BIND_PORT;
                    	}

                    	logger.debug(String.format("Subscriber for topic:%s discovered in EB:%s domain\n",
                    			topic,eb_address));
                    	
                    	//Add this region to topic t's subscribing regions set
                    	if(topic_subscribingRegions.containsKey(topic)){
                    		topic_subscribingRegions.get(topic).add(eb_address);
                    	}else{
                    		topic_subscribingRegions.put(topic, new HashSet<String>(Arrays.asList(eb_address)));
                    	}
                    	
                    	//check if this newly discovered region already exists as peer
                    	if(!region_subscribedTopics_map.containsKey(eb_address)){
                    		region_subscribedTopics_map.put(eb_address, new HashSet<String>());
                    		region_subscribedTopics_map.get(eb_address).add(topic);
                    		logger.debug(String.format("Adding eb:%s as peer for RB_P2_BIND_PORT:%s\n",
                    				eb_locator,RB_P2_BIND_PORT));
                    		rs.addPeer(domainRouteName, eb_locator, false);
                    		
                    	}else{
                    		logger.debug(String.format("eb:%s already exists as peer for RB_P2_BIND_PORT:%s\n", 
                    				eb_locator,RB_P2_BIND_PORT));
                    		region_subscribedTopics_map.get(eb_address).add(topic);
                    	}

                        // if topic route is not created, it needs to be created
                        if (!subscribedTopics.contains(topic)) {
                            subscribedTopics.add(topic);
                            //check if publishers for this topic exist
                            if(publishedTopics.contains(topic)){
                            	//Create topic route only if it does not already exist
                            	if(!topicRoutesList.contains(topic)){
                            		Set<String> publishing_regions= topic_publishingRegions.get(topic);
                            		Set<String> subscribing_regions= topic_subscribingRegions.get(topic);
                            	
                            		//Determine whether to create a topic route or not
                            		if(publishing_regions.size()==1 && subscribing_regions.size()==1 &&
                            				publishing_regions.containsAll(subscribing_regions)){
                            			//topic route is not created when publishers and subscribers for topic t exist in the same region
                            			logger.debug(String.format("Publishers and Subscribers for topic:%s exist in the same region. "
                            					+ "Will not create topic session", topic));
                            		}else{
                            			// topic route is created only when publishers and subscribers for topic t exist in different regions
                            			logger.debug(String.format("Both publishers and subscribers for topic:%s exist,"
                            					+ " creating Topic Session for topic:%s\n", topic, topic));
                            			rs.createTopicSession(domainRouteName, subscription_builtin_data.topic_name,
                            					subscription_builtin_data.type_name, TopicSession.SUBSCRIPTION_SESSION);
                            			topicRoutesList.add(topic);
                            		}
                            	}
                            }
                        }
                        break;
                    }
                    //called when a subscribing region for topic t leaves
                    case CHILD_REMOVED: {
                    	String eb_path=event.getData().getPath();
                    	String topic= eb_path.split("/")[2];
                    	String eb_address= eb_path.split("/")[4];
                    	String eb_locator;
                    	if(emulated_broker){
                    		eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_SUB_BIND_PORT;
                    	}
                    	else{
                    		eb_locator="tcpv4_wan://"+eb_address+":"+EB_P2_BIND_PORT;
                    	}

                    	logger.debug(String.format("EB:%s was removed\n", eb_path));
                    	
                    	//remove this region from topic t's set of subscribing regions
                    	if(topic_subscribingRegions.containsKey(topic)){
                    		topic_subscribingRegions.get(topic).remove(eb_address);
                    	}
                    	
                    	if(region_subscribedTopics_map.containsKey(eb_address)){
                    		region_subscribedTopics_map.get(eb_address).remove(topic);
                    		if(region_subscribedTopics_map.get(eb_address).size()==0){
                    			//Not interfacing with this region for any subscribed topics then remove this region as peer
                    			rs.removePeer(domainRouteName, eb_locator, false);
                    			region_subscribedTopics_map.remove(eb_address);
                    		}
                    	}
                    	
                    	SubscriptionBuiltinTopicData subscription_builtin_topic_data=
                    			(SubscriptionBuiltinTopicData) CuratorHelper.deserialize(event.getData().getData());

                    	//get updated number of subscribing regions for topic t
                    	PathChildrenCache topic_sub_children_cache= subscribedTopic_childrenCache_map.get(topic);
                    	topic_sub_children_cache.rebuild();

                    	if(topic_sub_children_cache.getCurrentData().isEmpty()){
                    		logger.debug(String.format("There are no subscribing domains for topic:%s\n", topic));
                    		subscribedTopics.remove(topic);
                    		//if there are no subscribing regions for topic t, remove its topic session
                    		if(topicRoutesList.contains(topic)){
                    			topicRoutesList.remove(topic);
                    			logger.debug(String.format("Subscribing domains for topic:%s do not exist.\n"
                    					+ "Removing topic session:%s\n",topic,String.format("%s::%sTopicSession",
                    							domainRouteName,subscription_builtin_topic_data.topic_name)));
                    			rs.deleteTopicSession(domainRouteName,subscription_builtin_topic_data.topic_name,
                    					subscription_builtin_topic_data.type_name,TopicSession.SUBSCRIPTION_SESSION);
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
}
