package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
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
	public static String RB_DOMAIN_ROUTE="RB_DOMAIN_ROUTE";
	public static String EB_DOMAIN_ROUTE="EB_DOMAIN_ROUTE";
	public static String EB_PUB_DOMAIN_ROUTE="EB_PUB_DOMAIN_ROUTE";
	public static String EB_SUB_DOMAIN_ROUTE="EB_SUB_DOMAIN_ROUTE";
	public static String EB_LOCAL_DOMAIN_ROUTE="EB_LOCAL_DOMAIN_ROUTE";
	
	
	private static final String TYPES_PACKAGE = "com.rti.idl.test";
	
	private String domainRouteName;
	private DefaultParticipant firstParticipant;
	private DefaultParticipant secondParticipant; 
	private Map<String,TopicSession<?>> subscription_topic_sessions;
	private Map<String,TopicSession<?>> publication_topic_sessions;

	
	public DomainRoute(String domainRouteName,String type) throws Exception{
		this.domainRouteName=domainRouteName;

		subscription_topic_sessions=new HashMap<String, TopicSession<?>>();
		publication_topic_sessions=new HashMap<String, TopicSession<?>>();
		
		if(type.equals(RB_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(RoutingBroker.WAN_DOMAIN_ID,
					participantQos(RoutingBroker.RB_P1_BIND_PORT));
			secondParticipant= new DefaultParticipant(RoutingBroker.WAN_DOMAIN_ID,
					participantQos(RoutingBroker.RB_P2_BIND_PORT));
		}else if(type.equals(EB_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(EdgeBroker.DEFAULT_DOMAIN_ID);
			secondParticipant= new DefaultParticipant(EdgeBroker.WAN_DOMAIN_ID,
					participantQos(EdgeBroker.EB_P2_BIND_PORT));	
		}else if(type.equals(EB_PUB_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(EdgeBroker.PUB_DOMAIN_ID);
			secondParticipant= new DefaultParticipant(EdgeBroker.WAN_DOMAIN_ID,
					participantQos(EdgeBroker.EB_P2_PUB_BIND_PORT));	
		}else if(type.equals(EB_SUB_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(EdgeBroker.SUB_DOMAIN_ID);
			secondParticipant= new DefaultParticipant(EdgeBroker.WAN_DOMAIN_ID,
					participantQos(EdgeBroker.EB_P2_SUB_BIND_PORT));	
		}else if(type.equals(EB_LOCAL_DOMAIN_ROUTE)){
			firstParticipant=new DefaultParticipant(EdgeBroker.PUB_DOMAIN_ID);
			secondParticipant= new DefaultParticipant(EdgeBroker.SUB_DOMAIN_ID);	
		}
		else{
			System.out.println("Domain Route type not recognized");
			throw new Exception(String.format("Domain Route Type:%s not recognized",type));
		}
	}
	
	public void addPeer(String locator, boolean on_first_participant) {
		if (on_first_participant) {
			firstParticipant.add_peer(locator);
		} else {
			secondParticipant.add_peer(locator);
		}
	}
	
	public void removePeer(String locator, boolean on_first_participant) {
		if (on_first_participant) {
			firstParticipant.remove_peer(locator);
		} else {
			secondParticipant.remove_peer(locator);
		}
	}

	@SuppressWarnings("rawtypes")
	public void createTopicSession(String topicName,String typeName,String session_type) throws Exception
	{
		if(session_type.equals(TopicSession.SUBSCRIPTION_SESSION)){
			//only create subscription topic session if it does not exist
			if(!subscription_topic_sessions.containsKey(topicName)){
				if (!publication_topic_sessions.containsKey(topicName)) {
					TypeSupportImpl typeSupport = get_type_support_instance(typeName);
					firstParticipant.registerType(typeSupport);
					Topic t1 = firstParticipant.create_topic(topicName, typeSupport);
					secondParticipant.registerType(typeSupport);
					Topic t2 = secondParticipant.create_topic(topicName, typeSupport);
					subscription_topic_sessions.put(topicName,
							new TopicSession(session_type, t1, t2, typeSupport,
									firstParticipant, secondParticipant));

				} else {
					//Since publication topic session already exists in this domain route, 
					//we don't need to register types or create topics
					TopicSession<?> session = publication_topic_sessions.get(topicName);
					subscription_topic_sessions.put(topicName,
							new TopicSession(session_type, session.t1,session.t2,session.typeSupport,
									firstParticipant, secondParticipant));
				}
			}
		}else if(session_type.equals(TopicSession.PUBLICATION_SESSION)){
			//only create publication topic session if it does not exist
			if(!publication_topic_sessions.containsKey(topicName)){
				if (!subscription_topic_sessions.containsKey(topicName)) {
					TypeSupportImpl typeSupport = get_type_support_instance(typeName);
					firstParticipant.registerType(typeSupport);
					Topic t1 = firstParticipant.create_topic(topicName, typeSupport);
					secondParticipant.registerType(typeSupport);
					Topic t2 = secondParticipant.create_topic(topicName, typeSupport);
					publication_topic_sessions.put(topicName,
							new TopicSession(session_type, t1, t2, typeSupport,
									firstParticipant, secondParticipant));

				} else {
					//Since subscription topic session already exists in this domain route, 
					//we don't need to regiter types or create topics
					TopicSession<?> session = subscription_topic_sessions.get(topicName);
					publication_topic_sessions.put(topicName,
							new TopicSession(session_type, session.t1,session.t2,session.typeSupport,
									firstParticipant, secondParticipant));
				}
			}
		}else{
			System.out.format("Session type:%s not recognized\n",session_type);
			throw new Exception(String.format("Session type:%s not recognized",session_type));
		}
	}	
	
	public void deleteTopicSession(String topic_name,String type_name,String session_type) throws Exception{
		if(session_type.equals(TopicSession.SUBSCRIPTION_SESSION)){
			TopicSession<?> session=subscription_topic_sessions.remove(topic_name);
			session.deleteEndpoints();
			if(!publication_topic_sessions.containsKey(topic_name)){
				firstParticipant.delete_topic(session.t1);
				secondParticipant.delete_topic(session.t2);
				firstParticipant.unregisterType(type_name);
				secondParticipant.unregisterType(type_name);
			}
		}else if(session_type.equals(TopicSession.PUBLICATION_SESSION)){
			TopicSession<?> session=publication_topic_sessions.remove(topic_name);
			session.deleteEndpoints();
			if(!subscription_topic_sessions.containsKey(topic_name)){
				firstParticipant.delete_topic(session.t1);
				secondParticipant.delete_topic(session.t2);
				firstParticipant.unregisterType(type_name);
				secondParticipant.unregisterType(type_name);
			}
		}else{
			throw new Exception(String.format("session_type:%s not recoginzed",session_type));
		}
	}
	
	private DomainParticipantQos participantQos(String port) throws Exception
	{
		String address = InetAddress.getLocalHost().getHostAddress();

		DomainParticipantQos participant_qos = new DomainParticipantQos();
		DomainParticipantFactory.TheParticipantFactory.get_default_participant_qos(participant_qos);

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

}
