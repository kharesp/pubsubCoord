package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.net.InetAddress;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.rti.dds.domain.DomainParticipant;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.domain.DomainParticipantFactoryQos;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.publication.builtin.PublicationBuiltinTopicDataDataReader;
import com.rti.dds.publication.builtin.PublicationBuiltinTopicDataTypeSupport;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.subscription.builtin.SubscriptionBuiltinTopicDataDataReader;
import com.rti.dds.subscription.builtin.SubscriptionBuiltinTopicDataTypeSupport;
import edu.vanderbilt.kharesp.pubsubcoord.routing.DomainRoute;
import edu.vanderbilt.kharesp.pubsubcoord.routing.RoutingServiceAdministrator;

public class EdgeBroker {
	// Domain id in which Routing Brokers operate in the cloud
	public static final int WAN_DOMAIN_ID = 230;
	// Default domain id for each region
	public static final int DEFAULT_DOMAIN_ID=0;
	// Domain id in which publishers operate in this local domain
	public static final int PUB_DOMAIN_ID=0;
	// Domain id in which subscribers operate in this local domain
	public static final int SUB_DOMAIN_ID=1;
    // public facing port for sending/receiving data for this local domain
    public static final String EB_P2_BIND_PORT = "8502";
    public static final String EB_P2_PUB_BIND_PORT = "8502";
    public static final String EB_P2_SUB_BIND_PORT = "8503";

    //Domain route name prefixes
    public static final String DOMAIN_ROUTE_NAME_PREFIX = "EdgeBrokerDomainRoute";
    public static final String LOCAL_DOMAIN_ROUTE_NAME_PREFIX = "LocalEdgeBrokerDomainRoute";
    public static final String PUB_DOMAIN_ROUTE_NAME_PREFIX = "PubEdgeBrokerDomainRoute";
    public static final String SUB_DOMAIN_ROUTE_NAME_PREFIX = "SubEdgeBrokerDomainRoute";

    private String ebAddress;

    //Names of domain routes maintained by the EB
    private String domainRouteName; 
    private String localDomainRouteName;
    private String pubDomainRouteName;
    private String subDomainRouteName;
    
    
    private boolean emulated_broker;
    private RoutingServiceAdministrator rs;
    private CuratorHelper client = null;
    private Logger logger;

    public EdgeBroker(String zkConnector,boolean emulated_broker){
    	this.emulated_broker=emulated_broker;
    	logger=Logger.getLogger(this.getClass().getSimpleName());

        try {
             ebAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
            System.out.println("Host address is not known");
        }

        // Connect to ZK
    	client = new CuratorHelper(zkConnector);
    	
        // Create Routing Service remote administrator 
        try {
			rs=new RoutingServiceAdministrator();
		} catch (Exception e) {
            logger.error(e.getMessage(),e);
		}
    }
    public static void main(String args[]){
    	if (args.length<2){
    		System.out.println("Enter zkConnector (address:port), emulated_broker(0/1)");
    		return;
    	}
    	String zkConnector=args[0];
    	int emulated_broker=Integer.parseInt(args[1]);
    	PropertyConfigurator.configure("log4j.properties");
    	new EdgeBroker(zkConnector,emulated_broker>0?true:false).start();
    }
    
    public void start(){
    	logger.debug(String.format("Starting EdgeBroker:%s\n",ebAddress));
    	
    	//domain route names
    	domainRouteName=DOMAIN_ROUTE_NAME_PREFIX + "@"+ ebAddress;
    	localDomainRouteName=LOCAL_DOMAIN_ROUTE_NAME_PREFIX + "@"+ ebAddress;
    	pubDomainRouteName=PUB_DOMAIN_ROUTE_NAME_PREFIX + "@"+ ebAddress;
    	subDomainRouteName=SUB_DOMAIN_ROUTE_NAME_PREFIX + "@"+ ebAddress;

    	//if EB is emulating a broker, create local, publisher and subscriber domain routes
    	if (emulated_broker){
    		//Create local domain route between this EB's publishing domain and subscribing domain
    		logger.debug(String.format("EB:%s will create a DomainRoute:%s between publisher's local domain id:%d and subscriber's local domain id:%d\n",
        			ebAddress,localDomainRouteName,PUB_DOMAIN_ID,SUB_DOMAIN_ID));
        	rs.createDomainRoute(localDomainRouteName,DomainRoute.EB_LOCAL_DOMAIN_ROUTE);
        	
        	//Create domain route between EB's publishing domain and WAN domain
    		logger.debug(String.format("EB:%s will create a DomainRoute:%s between publisher's local domain id:%d and wan domain id:%d\n",
        			ebAddress,pubDomainRouteName,PUB_DOMAIN_ID,WAN_DOMAIN_ID));
        	rs.createDomainRoute(subDomainRouteName,DomainRoute.EB_PUB_DOMAIN_ROUTE);
    		
    		//Create domain route between EB's subscribing domain and WAN domain
    		logger.debug(String.format("EB:%s will create a DomainRoute:%s between subscriber's local domain id:%d and wan domain id:%d\n",
        			ebAddress,subDomainRouteName,SUB_DOMAIN_ID,WAN_DOMAIN_ID));
        	rs.createDomainRoute(subDomainRouteName,DomainRoute.EB_SUB_DOMAIN_ROUTE);
    		
    	}else{
    		//Create domain route between EB domain and WAN domain
    		logger.debug(String.format("EB:%s will create a DomainRoute:%s between local domain id:%d and wan domain id:%d\n",
        			ebAddress,domainRouteName,DEFAULT_DOMAIN_ID,WAN_DOMAIN_ID));
        	
        	rs.createDomainRoute(domainRouteName,DomainRoute.EB_DOMAIN_ROUTE);
    	}
        
        try{
        	// Ensure /topics path exists 
        	client.ensurePathExists(CuratorHelper.TOPIC_PATH);

        	// Create built-in entities to listen for endpoint creation
        	if(emulated_broker){
        		listenToBuiltinTopics_brokered();
        	}
        	else{
        		listenToBuiltinTopics();
        	}

        	while (true) {
        		Thread.sleep(1000);
        	}
        }catch(Exception e){
        	logger.error(e.getMessage(),e);    	
        }finally{
        	client.close();
        }
    }

    private void listenToBuiltinTopics() throws Exception {
		logger.debug(String.format("EdgeBroker:%s installing listeners for builtin topics\n", ebAddress));

		//Create Domain Participant with builtin entities disabled
		DomainParticipant participant = get_participant_with_disabled_entities(DEFAULT_DOMAIN_ID);
		if (participant == null) {
			throw new Exception(String.format("Builtin Topics Participant creation failed for domain id:%d\n",
					DEFAULT_DOMAIN_ID));
		}

		//install listener to listen for publisher creation
		install_builtin_publisher_listener(participant);
		
		//install listener to listen for subscriber creation
		install_builtin_subscriber_listener(participant);
		
		//enable all entities within the participant
		participant.enable();
    }

    private void listenToBuiltinTopics_brokered() throws Exception {
		logger.debug(String.format("EdgeBroker:%s installing listeners for builtin topics\n", ebAddress));
		
		//Create Domain Participant with builtin entities disabled for publishing domain 
		DomainParticipant participant_for_pub = get_participant_with_disabled_entities(PUB_DOMAIN_ID);
		if (participant_for_pub == null) {
			throw new Exception(String.format("Builtin Topics Participant creation failed for domain id:%d\n",
					PUB_DOMAIN_ID));
		}

		//Create Domain Participant with builtin entities disabled for subscribing domain 
		DomainParticipant participant_for_sub = get_participant_with_disabled_entities(SUB_DOMAIN_ID);
		if (participant_for_sub==null) {
			throw new Exception(String.format("Builtin Topics Participant creation failed for domain id:%d\n",
					SUB_DOMAIN_ID));
		}
		
		//install listener to listen for publisher creation
		install_builtin_publisher_listener(participant_for_pub);

		//install listener to listen for subscriber creation
		install_builtin_subscriber_listener(participant_for_sub);
		
		//enable builtin entities within participants
		participant_for_pub.enable();
		participant_for_sub.enable();
    }
   
    
    private void install_builtin_publisher_listener(DomainParticipant participant){
    	try{
    		// obtain builtin Subscriber
    		Subscriber builtin_subscriber = participant.get_builtin_subscriber();
			if (builtin_subscriber == null) {
				throw new Exception("Builtin Subscriber creation failed");
			}

			// obtain DR to listen for publisher creation
			PublicationBuiltinTopicDataDataReader builtin_publication_datareader = (PublicationBuiltinTopicDataDataReader) builtin_subscriber
					.lookup_datareader(PublicationBuiltinTopicDataTypeSupport.PUBLICATION_TOPIC_NAME);
			if (builtin_publication_datareader == null) {
				throw new Exception("Built-in Publication DataReader creation failed");
			}
			
			logger.debug(String.format("EdgeBroker:%s installing listener for publisher discovery\n",ebAddress));

			// Install listener for Publication discovery
			BuiltinPublisherListener builtin_publisher_listener =
					new BuiltinPublisherListener(ebAddress,client,rs,emulated_broker);
			builtin_publication_datareader.set_listener(builtin_publisher_listener, StatusKind.STATUS_MASK_ALL);
			
    	}catch(Exception e){
    		if (participant != null) {
                participant.delete_contained_entities();
                DomainParticipantFactory.TheParticipantFactory.
                        delete_participant(participant);
            }
    	}
    }
    
    private void install_builtin_subscriber_listener(DomainParticipant participant){
		try {
			// obtain builtin subscriber
			Subscriber builtin_subscriber = participant.get_builtin_subscriber();
			if (builtin_subscriber == null) {
				throw new Exception("Subscriber creation failed");
			}

			// obtian DR to listen for subscriber creation
			SubscriptionBuiltinTopicDataDataReader builtin_subscription_datareader = (SubscriptionBuiltinTopicDataDataReader) builtin_subscriber
					.lookup_datareader(SubscriptionBuiltinTopicDataTypeSupport.SUBSCRIPTION_TOPIC_NAME);
			if (builtin_subscription_datareader == null) {
				throw new IllegalStateException("Built-in Subscription DataReader creation failed");
			}

			logger.debug(String.format("EdgeBroker:%s installing listener for subscriber discovery\n", ebAddress));

			// Install listener for Subscription discovery
			BuiltinSubscriberListener builtin_subscriber_listener = 
					new BuiltinSubscriberListener(ebAddress, client,rs,emulated_broker);
			builtin_subscription_datareader.set_listener(builtin_subscriber_listener, StatusKind.STATUS_MASK_ALL);

		} catch (Exception e) {
			if (participant != null) {
				participant.delete_contained_entities();
				DomainParticipantFactory.TheParticipantFactory.delete_participant(participant);
			}
		}
    }
    
    private DomainParticipant get_participant_with_disabled_entities(int domainId) {
    	//DomainParticipant QoS with disabled entities
		DomainParticipantFactoryQos factory_qos = new DomainParticipantFactoryQos();
		DomainParticipantFactory.TheParticipantFactory.get_qos(factory_qos);
		factory_qos.entity_factory.autoenable_created_entities = false;

		
		//Create Domain Participant with disabled entities
		DomainParticipantFactory.TheParticipantFactory.set_qos(factory_qos);
		DomainParticipant participant= DomainParticipantFactory.TheParticipantFactory.create_participant(domainId,
				DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);
		return participant;
    }

}
