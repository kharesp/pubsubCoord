package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.rti.idl.command.CommandTypeSupport;
import com.rti.dds.subscription.DataReaderQos;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.Subscriber;
import com.rti.idl.command.Command;

import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataReader;

public class RoutingService {
	public static final int RS_ADMIN_DOMAIN_ID = 55;
	public static final String COMMAND_TOPIC="command";
	public static final String TOPIC_ROUTE_STRING_CODE = "k";

	public static final String COMMAND_CREATE_DOMAIN_ROUTE="create_domain_route";
	public static final String COMMAND_CREATE_TOPIC_SESSION="create_topic_session";
	public static final String COMMAND_ADD_PEER="add_peer";
	public static final String COMMAND_DELETE_PEER="delete_peer";

	private String routingServiceName;
	private Map<String,DomainRoute> domain_routes;
	
	private DefaultParticipant participant;
	private GenericDataReader<Command> listener;
   
	private BlockingQueue<Command> queue;

	public RoutingService(String name) throws Exception{
		this.routingServiceName=name;
		this.domain_routes=new HashMap<String,DomainRoute>();
		queue= new LinkedBlockingQueue<Command>();
		participant= new DefaultParticipant(RS_ADMIN_DOMAIN_ID);
		participant.registerType(CommandTypeSupport.get_instance());
		Subscriber subscriber= participant.get_default_subscriber();
		DataReaderQos qos= new DataReaderQos();
		subscriber.get_default_datareader_qos(qos);
		qos.user_data.value.addAllByte(TOPIC_ROUTE_STRING_CODE.getBytes());
		listener = new GenericDataReader<Command>(subscriber,
				participant.create_topic(COMMAND_TOPIC, CommandTypeSupport.getInstance()),
				CommandTypeSupport.get_instance(),qos) {
			@Override
			public void process(Command sample, SampleInfo info) {
				try {
					queue.put(sample);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}

		};
		listener.receive();
		process();
	}
	public void process(){
		while (true) {
			try {
				Command sample = queue.take();
				System.out.println("Received command:" + sample.command);
				if (sample.command.equals(COMMAND_CREATE_DOMAIN_ROUTE)) {
					String[] args = sample.arguments.split(",");
					createDomainRoute(args[0], args[1]);
				} else if (sample.command.equals(COMMAND_CREATE_TOPIC_SESSION)) {
					String[] args = sample.arguments.split(",");
					createTopicSession(args[0], args[1], args[2], args[3]);
				} else if (sample.command.equals(COMMAND_ADD_PEER)) {
					String[] args = sample.arguments.split(",");
					addPeer(args[0], args[1], Boolean.parseBoolean(args[2]));

				} else if (sample.command.equals(COMMAND_DELETE_PEER)) {
					String[] args = sample.arguments.split(",");
					removePeer(args[0], args[1], Boolean.parseBoolean(args[2]));
				} else {
					System.out.println("Command not recognized");
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public void addPeer(String domainRouteName,String locator,boolean isFirstParticipant){
		domain_routes.get(domainRouteName).addPeer(locator, isFirstParticipant);
	}

	public void removePeer(String domainRouteName,String locator,boolean isFirstParticipant){
		domain_routes.get(domainRouteName).removePeer(locator, isFirstParticipant);
	}
	
	public void createDomainRoute(String domainRouteName,String type) throws Exception
	{
		DomainRoute route=new DomainRoute(domainRouteName,type);
		domain_routes.put(domainRouteName, route);
	}
	
	public void createTopicSession(String domainRouteName,String topic_name,
			String type_name,String session_type) throws Exception{
		domain_routes.get(domainRouteName).createTopicSession(topic_name, type_name, session_type);
	}

	public String getName()
	{
		return routingServiceName;
	}
	public static void main(String args[]){
		try {
			String address= InetAddress.getLocalHost().getHostAddress();
			RoutingService rs= new RoutingService(address);
			while(true){
				Thread.sleep(1000);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
