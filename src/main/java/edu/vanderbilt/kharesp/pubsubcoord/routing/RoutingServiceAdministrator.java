package edu.vanderbilt.kharesp.pubsubcoord.routing;

import com.rti.idl.CommandTypeSupport;
import com.rti.dds.publication.DataWriterQos;
import com.rti.dds.publication.Publisher;
import com.rti.idl.Command;
import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataWriter;

public class RoutingServiceAdministrator {
	//DDS endpoints to send commands to Routing Service  
	private DefaultParticipant participant;
	private GenericDataWriter<Command> writer;
	
	public RoutingServiceAdministrator() throws Exception{
		//DDS participant for sending commands to Routing Service operating in RS_ADMIN_DOMAIN_ID 
		participant= new DefaultParticipant(RoutingService.RS_ADMIN_DOMAIN_ID,TransportQos.shmemParticipantQos());
		//register Command type
		participant.registerType(CommandTypeSupport.get_instance());
		//publisher to send commands
		Publisher publisher= participant.get_default_publisher();
		//set user_data qos to specify this is an infrastructure entity
		DataWriterQos qos= new DataWriterQos();
		publisher.get_default_datawriter_qos(qos);
		qos.user_data.value.addAllByte(RoutingService.INFRASTRUCTURE_NODE_IDENTIFIER.getBytes());
		writer= new GenericDataWriter<Command>(publisher,
				participant.create_topic(RoutingService.COMMAND_TOPIC, CommandTypeSupport.get_instance()),qos);
	}
	
	public void createDomainRoute(String domainRouteName, String domainRouteType ) {
		Command command= new Command();
		command.command=RoutingService.COMMAND_CREATE_DOMAIN_ROUTE;
		command.arguments=String.format("%s,%s", domainRouteName,domainRouteType);
		writer.write(command);
	}
	
	public void addPeer(String domainRouteName,String peerLocator,boolean isFirstParticipant) {
		Command command= new Command();
		command.command=RoutingService.COMMAND_ADD_PEER;
		command.arguments=String.format("%s,%s,%s", domainRouteName,peerLocator,String.valueOf(isFirstParticipant));
		writer.write(command);
	}
	
	public void removePeer(String domainRouteName,String peerLocator,boolean isFirstParticipant){
		Command command= new Command();
		command.command=RoutingService.COMMAND_DELETE_PEER;
		command.arguments=String.format("%s,%s,%s", domainRouteName,peerLocator,String.valueOf(isFirstParticipant));
		writer.write(command);
	}
	
	public void createTopicSession(String domainRouteName,String topic_name,
			String type_name,String session_type) {
		Command command= new Command();
		command.command=RoutingService.COMMAND_CREATE_TOPIC_SESSION;
		command.arguments=String.format("%s,%s,%s,%s",domainRouteName,topic_name,type_name,session_type);
		writer.write(command);
	}
	
	public void deleteTopicSession(String domainRouteName,String topic_name, String type_name, String session_type){
		Command command= new Command();
		command.command= RoutingService.COMMAND_DELETE_TOPIC_SESSION;
		command.arguments=String.format("%s,%s,%s,%s",domainRouteName,topic_name,type_name,session_type);
		writer.write(command);
	}
}
