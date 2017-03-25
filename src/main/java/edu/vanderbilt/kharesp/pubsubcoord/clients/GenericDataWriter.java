package edu.vanderbilt.kharesp.pubsubcoord.clients;

import com.rti.dds.infrastructure.InstanceHandle_t;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.publication.DataWriter;
import com.rti.dds.publication.DataWriterQos;
import com.rti.dds.publication.Publisher;
import com.rti.dds.topic.Topic;

public class GenericDataWriter<T> {

    private Publisher publisher;
    private Topic topic;
    private DataWriter writer;
    private InstanceHandle_t instance_handle;
    private DataWriterQos writerQos;
	
	public GenericDataWriter(Publisher publisher,Topic topic) throws Exception {
		this.publisher=publisher;
		this.topic=topic;
		writerQos=null;
		initialize();
	}

	public GenericDataWriter(Publisher publisher,Topic topic,DataWriterQos qos) throws Exception {
		this.publisher=publisher;
		this.topic=topic;
		writerQos=qos;
		initialize();
	}
	
	private void initialize() throws Exception{
		if(writerQos==null){
			writer = publisher.create_datawriter(topic,Publisher.DATAWRITER_QOS_DEFAULT,
				null,StatusKind.STATUS_MASK_NONE);
		}else{
			writer = publisher.create_datawriter(topic,writerQos,
				null,StatusKind.STATUS_MASK_NONE);
		}
		if (writer == null) {
			throw new Exception("create_datawriter error\n");
		}
		instance_handle= InstanceHandle_t.HANDLE_NIL;
	}
	
	
	public void write(T sample){
		writer.write_untyped(sample, instance_handle);
	}

}
