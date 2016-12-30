package edu.vanderbilt.kharesp.pubsubcoord.clients;

import com.rti.dds.infrastructure.InstanceHandle_t;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.publication.DataWriter;
import com.rti.dds.publication.Publisher;
import com.rti.dds.topic.Topic;
import com.rti.dds.topic.TypeSupportImpl;

public class GenericPublisher<T> {
	private int domainId;
	private String topicName;

	private DefaultParticipant participant;
    private com.rti.dds.publication.Publisher publisher;
    private TypeSupportImpl typeSupport;
    private Topic topic;
    private DataWriter writer;
    private InstanceHandle_t instance_handle;
	
	public GenericPublisher(int domainId,String topicName,TypeSupportImpl typeSupport) throws Exception {
		this.domainId=domainId;
		this.topicName=topicName;
		this.typeSupport=typeSupport;
		initialize();
	}
	
	private void initialize() throws Exception{
		try {
			participant= new DefaultParticipant(domainId);
			participant.registerType(typeSupport);
		} catch (Exception e) {
			throw e;
		}
		publisher= participant.get_default_publisher();
		
		topic = participant.create_topic(topicName,typeSupport);
		
		writer = publisher.create_datawriter(topic,Publisher.DATAWRITER_QOS_DEFAULT,
				null,StatusKind.STATUS_MASK_NONE);
		if (writer == null) {
			throw new Exception("create_datawriter error\n");
		}
		instance_handle= InstanceHandle_t.HANDLE_NIL;
	}
	
	public void write(T sample){
		writer.write_untyped(sample, instance_handle);
	}
	public void cleanup(){
		participant.shutdown();
	}

}
