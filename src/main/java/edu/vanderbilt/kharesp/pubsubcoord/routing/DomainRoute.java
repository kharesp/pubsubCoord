package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.domain.DomainParticipantQos;
import com.rti.dds.infrastructure.PropertyQosPolicyHelper;
import com.rti.dds.infrastructure.TransportBuiltinKind;
import com.rti.dds.topic.Topic;
import com.rti.dds.topic.TypeSupportImpl;
import edu.vanderbilt.kharesp.pubsubcoord.brokers.EdgeBroker;
import edu.vanderbilt.kharesp.pubsubcoord.brokers.RoutingBroker;
import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;

public class DomainRoute {
	//Types of DomainRoutes
	public static String RB_DOMAIN_ROUTE="RB_DOMAIN_ROUTE";
	public static String EB_DOMAIN_ROUTE="EB_DOMAIN_ROUTE";
	public static String EB_PUB_DOMAIN_ROUTE="EB_PUB_DOMAIN_ROUTE";
	public static String EB_SUB_DOMAIN_ROUTE="EB_SUB_DOMAIN_ROUTE";
	public static String EB_LOCAL_DOMAIN_ROUTE="EB_LOCAL_DOMAIN_ROUTE";
	
	//Root package for DDS Datatypes
	private static final String TYPES_PACKAGE = "com.rti.idl";

	private Logger logger;
	private String domainRouteName;
	private String routeType;
	private DefaultParticipant firstParticipant;
	private DefaultParticipant secondParticipant; 

	//Map to keep track of SubscriptionTopicSessions in this DomainRoute
	private Map<String,TopicSession<?>> subscription_topic_sessions;
	//Map to keep track of PublicationTopicSessions in this DomainRoute
	private Map<String,TopicSession<?>> publication_topic_sessions;

	
	public DomainRoute(String domainRouteName,String routeType) throws Exception{
		this.domainRouteName=domainRouteName;
		this.routeType=routeType;

		logger= Logger.getLogger(this.getClass().getSimpleName());
		PropertyConfigurator.configure("log4j.properties");

		subscription_topic_sessions=new HashMap<String, TopicSession<?>>();
		publication_topic_sessions=new HashMap<String, TopicSession<?>>();
		
		
		if(routeType.equals(RB_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(RoutingBroker.WAN_DOMAIN_ID,
					tcpWanTransportQoS(RoutingBroker.RB_P1_BIND_PORT));
			secondParticipant= new DefaultParticipant(RoutingBroker.WAN_DOMAIN_ID,
					tcpWanTransportQoS(RoutingBroker.RB_P2_BIND_PORT));

		}else if(routeType.equals(EB_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(EdgeBroker.DEFAULT_DOMAIN_ID,
					udpv4TransportQos());
			secondParticipant= new DefaultParticipant(EdgeBroker.WAN_DOMAIN_ID,
					tcpWanTransportQoS(EdgeBroker.EB_P2_BIND_PORT));	

		}else if(routeType.equals(EB_PUB_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(EdgeBroker.PUB_DOMAIN_ID,
					udpv4TransportQos());
			secondParticipant= new DefaultParticipant(EdgeBroker.WAN_DOMAIN_ID,
					tcpWanTransportQoS(EdgeBroker.EB_P2_PUB_BIND_PORT));	

		}else if(routeType.equals(EB_SUB_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(EdgeBroker.SUB_DOMAIN_ID,
					udpv4TransportQos());
			secondParticipant= new DefaultParticipant(EdgeBroker.WAN_DOMAIN_ID,
					tcpWanTransportQoS(EdgeBroker.EB_P2_SUB_BIND_PORT));	

		}else if(routeType.equals(EB_LOCAL_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(EdgeBroker.PUB_DOMAIN_ID,
					udpv4TransportQos());
			secondParticipant= new DefaultParticipant(EdgeBroker.SUB_DOMAIN_ID,
					udpv4TransportQos());	
		}
		else{
			logger.error(String.format("Domain route:%s of route type:%s not recognized",
					domainRouteName,routeType));
			throw new Exception(String.format("Domain route:%s of route type:%s not recognized",domainRouteName,
					routeType));
		}
		logger.debug(String.format("Created domain route:%s of type:%s",
				domainRouteName,routeType));
	}
	
	public void addPeer(String locator, boolean on_first_participant) throws Exception {
		if (on_first_participant) {
			firstParticipant.add_peer(locator);
			logger.debug(String.format("Added peer:%s on first participant for domain route:%s",
					locator,domainRouteName));
		} else {
			secondParticipant.add_peer(locator);
			logger.debug(String.format("Added peer:%s on second participant for domain route:%s",
					locator,domainRouteName));
		}
	}
	
	public void removePeer(String locator, boolean on_first_participant) throws Exception{
		if (on_first_participant) {
			firstParticipant.remove_peer(locator);
			logger.debug(String.format("Removed peer:%s on first participant for domain route:%s",
					locator,domainRouteName));
		} else {
			secondParticipant.remove_peer(locator);
			logger.debug(String.format("Removed peer:%s on second participant for domain route:%s",
					locator,domainRouteName));
		}
	}

	@SuppressWarnings("rawtypes")
	public void createTopicSession(String topicName,String typeName,String sessionType) throws Exception
	{
		logger.debug(String.format("Creating topic sesssion for topic_name:%s,"
				+ " type_name:%s and session_type:%s in domain route:%s",
				topicName,typeName,sessionType,domainRouteName));

		if(sessionType.equals(TopicSession.SUBSCRIPTION_SESSION)){
			//only create subscription topic session if it does not exist
			if(!subscription_topic_sessions.containsKey(topicName)){
				if (!publication_topic_sessions.containsKey(topicName)) {
					//Register types and create topics
					TypeSupportImpl typeSupport = get_type_support_instance(typeName);

					firstParticipant.registerType(typeSupport);
					logger.debug(String.format("Registered type:%s for first participant in domain route:%s",
							typeSupport.get_type_nameI(),domainRouteName));
					Topic t1 = firstParticipant.create_topic(topicName, typeSupport);
					logger.debug(String.format("Created topic:%s for first participant in domain route:%s",
							topicName,domainRouteName));

					secondParticipant.registerType(typeSupport);
					logger.debug(String.format("Registered type:%s for second participant in domain route:%s",
							typeSupport.get_type_nameI(),domainRouteName));
					Topic t2 = secondParticipant.create_topic(topicName, typeSupport);
					logger.debug(String.format("Created topic:%s for second participant in domain route:%s",
							topicName,domainRouteName));

					subscription_topic_sessions.put(topicName,
							new TopicSession(sessionType, t1, t2, typeSupport,
									firstParticipant, secondParticipant));

				} else {
					//Since publication topic session already exists in this domain route, 
					//we don't need to register types or create topics
					TopicSession<?> session = publication_topic_sessions.get(topicName);
					subscription_topic_sessions.put(topicName,
							new TopicSession(sessionType, session.t1,session.t2,session.typeSupport,
									firstParticipant, secondParticipant));

				}
			}else{
				logger.error(String.format("%s topic session %s already exists in domain route:%s",
						sessionType,topicName,domainRouteName));
			}
		}else if(sessionType.equals(TopicSession.PUBLICATION_SESSION)){
			//only create publication topic session if it does not exist
			if(!publication_topic_sessions.containsKey(topicName)){
				if (!subscription_topic_sessions.containsKey(topicName)) {
					//Register types and create topics
					TypeSupportImpl typeSupport = get_type_support_instance(typeName);

					firstParticipant.registerType(typeSupport);
					logger.debug(String.format("Registered type:%s for first participant in domain route:%s",
							typeSupport.get_type_nameI(),domainRouteName));
					Topic t1 = firstParticipant.create_topic(topicName, typeSupport);
					logger.debug(String.format("Created topic:%s for first participant in domain route:%s",
							topicName,domainRouteName));

					secondParticipant.registerType(typeSupport);
					logger.debug(String.format("Registered type:%s for second participant in domain route:%s",
							typeSupport.get_type_nameI(),domainRouteName));
					Topic t2 = secondParticipant.create_topic(topicName, typeSupport);
					logger.debug(String.format("Created topic:%s for second participant in domain route:%s",
							topicName,domainRouteName));
					publication_topic_sessions.put(topicName,
							new TopicSession(sessionType, t1, t2, typeSupport,
									firstParticipant, secondParticipant));

				} else {
					//Since subscription topic session already exists in this domain route, 
					//we don't need to register types or create topics
					TopicSession<?> session = subscription_topic_sessions.get(topicName);
					publication_topic_sessions.put(topicName,
							new TopicSession(sessionType, session.t1,session.t2,session.typeSupport,
									firstParticipant, secondParticipant));
				}
			}else{
				logger.error(String.format("%s topic session %s already exists in domain route:%s",
						sessionType,topicName,domainRouteName));
			}
		}else{
			logger.error(String.format("Session type:%s for topic:%s in domain route:%s not recognized",
					sessionType,topicName,domainRouteName));
			throw new Exception(String.format("Session type:%s for topic:%s in domain route:%s not recognized",
					sessionType,topicName,domainRouteName));
		}
		logger.debug(String.format("Created topic sesssion for topic_name:%s,"
				+ " type_name:%s and session_type:%s in domain route:%s",
				topicName,typeName,sessionType,domainRouteName));
	}	
	
	public void deleteTopicSession(String topic_name,String type_name,String session_type) throws Exception{
		logger.debug(String.format("Deleting %s topic session of type:%s from domain route:%s",
				topic_name, session_type, domainRouteName));

		if(session_type.equals(TopicSession.SUBSCRIPTION_SESSION)){
			TopicSession<?> session=subscription_topic_sessions.remove(topic_name);
			session.deleteEndpoints();

			if(!publication_topic_sessions.containsKey(topic_name)){
				//since publication session for the same topic name does not exist, delete topic and unregister type
				firstParticipant.delete_topic(session.t1);
				logger.debug(String.format("Deleted %s topic from first participant in domain route:%s",
						topic_name,domainRouteName));
				secondParticipant.delete_topic(session.t2);
				logger.debug(String.format("Deleted %s topic from second participant in domain route:%s",
						topic_name,domainRouteName));
				firstParticipant.unregisterType(type_name);
				logger.debug(String.format("Unregistered type:%s from first participant in domain route:%s",
						type_name,domainRouteName));
				secondParticipant.unregisterType(type_name);
				logger.debug(String.format("Unregistered type:%s from second participant in domain route:%s",
						type_name,domainRouteName));
			}
		}else if(session_type.equals(TopicSession.PUBLICATION_SESSION)){
			TopicSession<?> session=publication_topic_sessions.remove(topic_name);
			session.deleteEndpoints();
			if(!subscription_topic_sessions.containsKey(topic_name)){
				//since subscription session for the same topic name does not exist, delete topic and unregister type
				firstParticipant.delete_topic(session.t1);
				logger.debug(String.format("Deleted %s topic from first participant in domain route:%s",
						topic_name,domainRouteName));

				secondParticipant.delete_contentfilteredtopic(session.cft);
				secondParticipant.delete_topic(session.t2);
				logger.debug(String.format("Deleted %s topic from second participant in domain route:%s",
						topic_name,domainRouteName));

				firstParticipant.unregisterType(type_name);
				logger.debug(String.format("Unregistered type:%s from first participant in domain route:%s",
						type_name,domainRouteName));
				secondParticipant.unregisterType(type_name);
				logger.debug(String.format("Unregistered type:%s from second participant in domain route:%s",
						type_name,domainRouteName));
			}
		}else{
			logger.error(String.format("Failed to delete topic session:%s in domain route:%s. Session type:%s not recognized",
					topic_name,domainRouteName,session_type));
			throw new Exception(String.format("Failed to delete topic session:%s in domain route:%s. Session type:%s not recognized",
					topic_name,domainRouteName,session_type));
		}
		logger.debug(String.format("Deleted %s topic session of type:%s from domain route:%s",
				topic_name, session_type, domainRouteName));
	}
	
	
	private DomainParticipantQos tcpWanTransportQoS(String port) throws Exception
	{
		DomainParticipantQos participant_qos = new DomainParticipantQos();
		DomainParticipantFactory.TheParticipantFactory.get_default_participant_qos(participant_qos);
		String address = InetAddress.getLocalHost().getHostAddress();

		participant_qos.transport_builtin.mask = TransportBuiltinKind.MASK_NONE;
		PropertyQosPolicyHelper.add_property(participant_qos.property, "dds.transport.load_plugins",
				"dds.transport.TCPv4.tcp1", false);
		PropertyQosPolicyHelper.add_property(participant_qos.property, "dds.transport.TCPv4.tcp1.library",
				"nddstransporttcp", false);
		PropertyQosPolicyHelper.add_property(participant_qos.property, "dds.transport.TCPv4.tcp1.create_function",
				"NDDS_Transport_TCPv4_create", false);
		PropertyQosPolicyHelper.add_property(participant_qos.property, "dds.transport.TCPv4.tcp1.parent.classid",
				"NDDS_TRANSPORT_CLASSID_TCPV4_WAN", false);
		PropertyQosPolicyHelper.add_property(participant_qos.property, "dds.transport.TCPv4.tcp1.public_address",
				address + ":" + port, false);
		PropertyQosPolicyHelper.add_property(participant_qos.property, "dds.transport.TCPv4.tcp1.server_bind_port",
				port, false);
		return participant_qos;
	}
	
	private DomainParticipantQos udpv4TransportQos() throws Exception
	{
		DomainParticipantQos participant_qos = new DomainParticipantQos();
		DomainParticipantFactory.TheParticipantFactory.get_default_participant_qos(participant_qos);
		participant_qos.transport_builtin.mask = TransportBuiltinKind.UDPv4;
		return participant_qos;
	}
	
	private TypeSupportImpl get_type_support_instance(String type_name) {
		try {
			Class<?> type_support_class = Class.forName(TYPES_PACKAGE + "." + type_name+"TypeSupport");
			Method getInstance = type_support_class.getMethod("get_instance");
			return (TypeSupportImpl) getInstance.invoke(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public String getName(){
		return domainRouteName;
	}
	
	
	public String getType(){
		return routeType;
	}

}
