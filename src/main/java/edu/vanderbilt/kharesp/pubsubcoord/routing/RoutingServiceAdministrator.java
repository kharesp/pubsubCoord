package edu.vanderbilt.kharesp.pubsubcoord.routing;

import com.rti.idl.command.CommandTypeSupport;
import com.rti.dds.publication.DataWriterQos;
import com.rti.dds.publication.Publisher;
import com.rti.idl.command.Command;

import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataWriter;

public class RoutingServiceAdministrator {
	private DefaultParticipant participant;
	private GenericDataWriter<Command> writer;
	
	public RoutingServiceAdministrator(String name) throws Exception{
		participant= new DefaultParticipant(RoutingService.RS_ADMIN_DOMAIN_ID);
		participant.registerType(CommandTypeSupport.get_instance());
		Publisher publisher= participant.get_default_publisher();
		DataWriterQos qos= new DataWriterQos();
		publisher.get_default_datawriter_qos(qos);
		qos.user_data.value.addAllByte(RoutingService.TOPIC_ROUTE_STRING_CODE.getBytes());
		writer= new GenericDataWriter<Command>(publisher,
				participant.create_topic(RoutingService.COMMAND_TOPIC, CommandTypeSupport.get_instance()),qos);
	}
	
	public void close(){
		participant.shutdown();
	}
	
	public void removePeer(String domainRouteName,String peerLocator,boolean isFirstParticipant){
		Command command= new Command();
		command.command=RoutingService.COMMAND_DELETE_PEER;
		command.arguments=String.format("%s,%s,%s", domainRouteName,peerLocator,String.valueOf(isFirstParticipant));
		writer.write(command);
	}
	
	public void addPeer(String domainRouteName,String peerLocator,boolean isFirstParticipant) {
		Command command= new Command();
		command.command=RoutingService.COMMAND_ADD_PEER;
		command.arguments=String.format("%s,%s,%s", domainRouteName,peerLocator,String.valueOf(isFirstParticipant));
		writer.write(command);
	}
	
	public void createDomainRoute(String name, String type ) {
		Command command= new Command();
		command.command=RoutingService.COMMAND_CREATE_DOMAIN_ROUTE;
		command.arguments=String.format("%s,%s", name,type);
		writer.write(command);
	}

	public void createTopicSession(String domainRouteName,String topic_name,
			String type_name,String session_type) {
		Command command= new Command();
		command.command=RoutingService.COMMAND_CREATE_TOPIC_SESSION;
		command.arguments=String.format("%s,%s,%s,%s",domainRouteName,topic_name,type_name,session_type);
		writer.write(command);
	}
}
