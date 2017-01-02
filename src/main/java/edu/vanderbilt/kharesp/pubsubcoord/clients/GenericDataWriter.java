package edu.vanderbilt.kharesp.pubsubcoord.clients;

import com.rti.dds.domain.DomainParticipant;
import com.rti.dds.infrastructure.InstanceHandle_t;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.publication.DataWriter;
import com.rti.dds.publication.Publisher;
import com.rti.dds.topic.Topic;
import com.rti.dds.topic.TypeSupportImpl;

public class GenericDataWriter<T> {

	private String topicName;
    private Publisher publisher;
    private TypeSupportImpl typeSupport;
    private Topic topic;
    private DataWriter writer;
    private InstanceHandle_t instance_handle;
	
	public GenericDataWriter(Publisher publisher,String topicName,
			TypeSupportImpl typeSupport) throws Exception {
		this.publisher=publisher;
		this.topicName=topicName;
		this.typeSupport=typeSupport;
		initialize();
	}
	
	private void initialize() throws Exception{
		DomainParticipant participant= publisher.get_participant();
		//Register type 
	    typeSupport.register_typeI(participant, typeSupport.get_type_nameI());
	    //Create Topic
	    topic=participant.create_topic(topicName,typeSupport.get_type_nameI(),
				DomainParticipant.TOPIC_QOS_DEFAULT, null,
				StatusKind.STATUS_MASK_NONE);
		if (topic == null) {
			throw new Exception("create_topic error\n");
		}
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

}
