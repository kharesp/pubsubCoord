package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.net.InetAddress;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.domain.DomainParticipantQos;
import com.rti.dds.infrastructure.PropertyQosPolicyHelper;
import com.rti.dds.infrastructure.TransportBuiltinKind;
import com.rti.idl.test.DataSample_64B;
import com.rti.idl.test.DataSample_64BTypeSupport;

import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataWriter;

public class EdgeRouting {
	public static void main(String args[]) {
		DefaultParticipant participant=null;
		try {
			String RB_P1_BIND_PORT="8500";
			String EB_P2_BIND_PORT = "8502";
			String ebAddress = InetAddress.getLocalHost().getHostAddress();
			String rb_address= "10.2.2.12";

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
					ebAddress + ":" + EB_P2_BIND_PORT, false);
			PropertyQosPolicyHelper.add_property(participant_qos.property, "dds.transport.TCPv4.tcp1.server_bind_port",
					EB_P2_BIND_PORT, false);

			participant = new DefaultParticipant(230, participant_qos);
			participant.participant().add_peer("tcpv4_wan://" + rb_address + ":" + RB_P1_BIND_PORT);
			GenericDataWriter<DataSample_64B> datawriter = new GenericDataWriter<DataSample_64B>(participant.get_default_publisher(),
					"test",
					DataSample_64BTypeSupport.get_instance());
			DataSample_64B instance = new DataSample_64B();
			for (int count = 0; count < 100; ++count) {
				instance.sample_id = count;
				instance.ts_milisec = System.currentTimeMillis();
				datawriter.write(instance);

				System.out.println("Sent sample:" + count);
				try {
					Thread.sleep(2000);
				} catch (InterruptedException ix) {
					System.err.println("INTERRUPTED");
					break;
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			participant.shutdown();
		}

	}
}
