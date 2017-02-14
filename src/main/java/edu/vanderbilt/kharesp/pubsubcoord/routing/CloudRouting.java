package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.net.InetAddress;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.domain.DomainParticipantQos;
import com.rti.dds.infrastructure.PropertyQosPolicyHelper;
import com.rti.dds.infrastructure.TransportBuiltinKind;
import com.rti.dds.publication.Publisher;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.Subscriber;
import com.rti.idl.test.DataSample_64B;
import com.rti.idl.test.DataSample_64BTypeSupport;
import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataReader;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataWriter;

public class CloudRouting {
	public static void main(String args[]) {
		DefaultParticipant participant=null;
		DefaultParticipant second_participant=null;
		try {
			String RB_P1_BIND_PORT = "8500"; 
			String rbAddress = InetAddress.getLocalHost().getHostAddress();

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
					rbAddress + ":" + RB_P1_BIND_PORT, false);
			PropertyQosPolicyHelper.add_property(participant_qos.property, "dds.transport.TCPv4.tcp1.server_bind_port",
					RB_P1_BIND_PORT, false);

			participant = new DefaultParticipant(230, participant_qos);
			second_participant= new DefaultParticipant(0);
			Publisher publisher= second_participant.get_default_publisher();
			Subscriber subscriber= participant.get_default_subscriber();
			GenericDataWriter<DataSample_64B> datawriter = 
					new GenericDataWriter<DataSample_64B>(publisher, "test",
					DataSample_64BTypeSupport.get_instance());

			GenericDataReader<DataSample_64B> datareader= new GenericDataReader<DataSample_64B>(subscriber,
    				"test",DataSample_64BTypeSupport.get_instance()){

						@Override
						public void process(DataSample_64B sample,SampleInfo info) {
                            long reception_ts=System.currentTimeMillis();
                            System.out.format("Received sample:%d at ts:%d. ts at which sample was sent:%d\n",
                            		sample.sample_id,reception_ts,sample.ts_milisec );
                            datawriter.write(sample);
						}
    		};
    		datareader.receive();
    		wait_for_data();
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			participant.shutdown();
			second_participant.shutdown();
		}
	}
	public static void wait_for_data(){
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println("Interrupted");
				break;
			}
		}
	   }
}
