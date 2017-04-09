package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.net.InetAddress;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.domain.DomainParticipantQos;
import com.rti.dds.infrastructure.PropertyQosPolicyHelper;
import com.rti.dds.infrastructure.TransportBuiltinKind;

public class TransportQos {

	//Shmem based communication only
	public static DomainParticipantQos shmemParticipantQos() throws Exception{
		DomainParticipantQos participant_qos = new DomainParticipantQos();
		DomainParticipantFactory.TheParticipantFactory.get_default_participant_qos(participant_qos);
		participant_qos.transport_builtin.mask = TransportBuiltinKind.SHMEM;
		return participant_qos;
	}

	//Udpv4 based communication only
	public static DomainParticipantQos udpv4Transport() throws Exception{
		DomainParticipantQos participant_qos = new DomainParticipantQos();
		DomainParticipantFactory.TheParticipantFactory.get_default_participant_qos(participant_qos);
		participant_qos.transport_builtin.mask = TransportBuiltinKind.UDPv4;
		return participant_qos;
	}
	
	//Tcp based WAN communication
	public static DomainParticipantQos tcpWanTransport(String port) throws Exception{
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

}
