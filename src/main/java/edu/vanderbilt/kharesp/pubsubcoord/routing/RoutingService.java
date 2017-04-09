package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import com.rti.idl.CommandTypeSupport;
import com.rti.dds.subscription.DataReaderQos;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.Subscriber;
import com.rti.idl.Command;
import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataReader;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class RoutingService {
	//Domain Id in which Routing Service operates in 
	public static final int RS_ADMIN_DOMAIN_ID = 55;
	//Topic on which Routing Service receives control commands
	public static final String COMMAND_TOPIC="command";
	//user_data qos identifier to distinguish infrastructure entities from client end-points 
	public static final String INFRASTRUCTURE_NODE_IDENTIFIER = "k";

	//Command types
	public static final String COMMAND_CREATE_DOMAIN_ROUTE="create_domain_route";
	public static final String COMMAND_ADD_PEER="add_peer";
	public static final String COMMAND_DELETE_PEER="delete_peer";
	public static final String COMMAND_CREATE_TOPIC_SESSION="create_topic_session";
	public static final String COMMAND_DELETE_TOPIC_SESSION="delete_topic_session";

	//regionId of the region within which this Routing Service is running
	public static int regionId;

	private Logger logger;
	private String routingServiceAddress;
	
	//Routing Service DDS endpoints to listen for incoming commands
	private DefaultParticipant participant;
	private GenericDataReader<Command> listener;
	private BlockingQueue<Command> queue;

	//Domain routes managed by this routing service instance
	private Map<String,DomainRoute> domain_routes;

	public RoutingService(String address) {
		logger= Logger.getLogger(this.getClass().getSimpleName());
		
		this.routingServiceAddress=address;
		regionId=Integer.parseInt(address.split("\\.")[2]);

		logger.debug(String.format("Starting Routing Serice instance at:%s in region:%d",
				routingServiceAddress,regionId));

		domain_routes=new HashMap<String,DomainRoute>();
		queue= new LinkedBlockingQueue<Command>();
		initializeListener();
	}

	private void initializeListener(){
		logger.debug("Initializing Routing Service entities");
		try{
			//Creating DDS Domain Participant
			participant= new DefaultParticipant(RS_ADMIN_DOMAIN_ID,TransportQos.shmemParticipantQos());
			//Registering Command Datatype
			participant.registerType(CommandTypeSupport.get_instance());
			//Creating a Subscriber to receive Commands
			Subscriber subscriber= participant.get_default_subscriber();
			//Setting user data qos to specify that this is an infrastructure end-point
			DataReaderQos qos = new DataReaderQos();
			subscriber.get_default_datareader_qos(qos);
			qos.user_data.value.addAllByte(INFRASTRUCTURE_NODE_IDENTIFIER.getBytes());
			
			//Create a DataReader and listener to receive Commands
			listener = new GenericDataReader<Command>(subscriber,
					participant.create_topic(COMMAND_TOPIC, CommandTypeSupport.getInstance()),
					CommandTypeSupport.get_instance(), qos) {
				@Override
				public void process(Command sample, SampleInfo info) {
					try {
						queue.put(sample);
					} catch (Exception e) {
						logger.error(e.getMessage(),e);
					}
				}
			};
			//start listening for incoming commands
			listener.receive();
		}catch(Exception e){
			logger.error(e.getMessage(),e);
		}
		
	}
	
	public void process(){
		while (true) {
			try {
				//take command from message queue or block if queue is empty
				Command sample = queue.take();
				String command= sample.command;
				String[] args = sample.arguments.split(",");
				
				logger.debug("***************");

				if (command.equals(COMMAND_CREATE_DOMAIN_ROUTE)) {
					logger.debug(String.format("Routing Service received Command:%s with arguments:"
							+ "domainRouteName:%s,domainRouteType:%s",command,args[0],args[1]));
					createDomainRoute(args[0], args[1]);
				} 
				else if (command.equals(COMMAND_ADD_PEER)) {
					logger.debug(String.format("Routing Service received Command:%s with arguments:"
							+ "domainRouteName:%s, peerLocator:%s, isFirstParticipant:%s",
							command,args[0],args[1],args[2]));
					addPeer(args[0], args[1], Boolean.parseBoolean(args[2]));
				}
				else if (command.equals(COMMAND_DELETE_PEER)) {
					logger.debug(String.format("Routing Service received Command:%s with arguments:"
							+ "domainRouteName:%s, peerLocator:%s, isFirstParticipant:%s",
							command,args[0],args[1],args[2]));
					removePeer(args[0], args[1], Boolean.parseBoolean(args[2]));
				}
				else if (command.equals(COMMAND_CREATE_TOPIC_SESSION)) {
					logger.debug(String.format("Routing Service received Command:%s with arguments:"
							+ "domainRouteName:%s,topicName:%s, typeName:%s, sessionType:%s",
							command,args[0],args[1],args[2],args[3]));
					createTopicSession(args[0], args[1], args[2], args[3]);
				}
				else if(command.equals(COMMAND_DELETE_TOPIC_SESSION)){
					logger.debug(String.format("Routing Service received Command:%s with arguments:"
							+ "domainRouteName:%s,topicName:%s, typeName:%s, sessionType:%s",
							command,args[0],args[1],args[2],args[3]));
					deleteTopicSession(args[0],args[1],args[2],args[3]);
				}
				else {
					logger.error(String.format("Routing Service received invalid command:%s", sample.command));
				}
			} catch (Exception e) {
				logger.error(e.getMessage(),e);
			}
		}
	}

	public void createDomainRoute(String domainRouteName,String type) throws Exception
	{
		DomainRoute route=new DomainRoute(domainRouteName,type);
		domain_routes.put(domainRouteName, route);
	}

	public void addPeer(String domainRouteName,String locator,boolean isFirstParticipant) throws Exception{
		DomainRoute route=domain_routes.getOrDefault(domainRouteName, null);
		if(route !=null){
			route.addPeer(locator, isFirstParticipant);
		}
		else{
			logger.error(String.format("addPeer error. domainRouteName:%s does not exist\n",domainRouteName));
		}
	}

	public void removePeer(String domainRouteName,String locator,boolean isFirstParticipant) throws Exception{
		DomainRoute route=domain_routes.getOrDefault(domainRouteName, null);
		if(route !=null){
			route.removePeer(locator, isFirstParticipant);
		}else{
			logger.error(String.format("removePeer error. domainRouteName:%s does not exist\n",domainRouteName));
		}
	}
	
	public void createTopicSession(String domainRouteName,String topic_name,
			String type_name,String session_type) throws Exception{

		DomainRoute route=domain_routes.getOrDefault(domainRouteName, null);
		if(route !=null){
			route.createTopicSession(topic_name, type_name, session_type);
		}else{
			logger.error(String.format("createTopicSession error. domainRouteName:%s does not exist\n",domainRouteName));
		}
	}
	
	public void deleteTopicSession(String domainRouteName, String topic_name,
			String type_name, String session_type) throws Exception{

		DomainRoute route=domain_routes.getOrDefault(domainRouteName, null);
		if(route !=null){
			route.deleteTopicSession(topic_name, type_name, session_type);
		}else{
			logger.error(String.format("deleteTopicSession error. domainRouteName:%s does not exist\n",domainRouteName));
		}
	}
	
	public static void main(String args[]){
		try {
			PropertyConfigurator.configure("log4j.properties");
			String address= InetAddress.getLocalHost().getHostAddress();
			RoutingService rs= new RoutingService(address);
			rs.process();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
