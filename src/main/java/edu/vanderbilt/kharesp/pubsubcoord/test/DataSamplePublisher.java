package edu.vanderbilt.kharesp.pubsubcoord.test;

import com.rti.dds.domain.*;
import com.rti.dds.infrastructure.*;
import com.rti.dds.publication.*;
import com.rti.dds.topic.*;

public class DataSamplePublisher {

    public static void main(String[] args) {
    	if (args.length < 4){
    		System.out.println("Usage: DataSamplePublisher domainId topicName sampleCount sendInterval");
    		return;
    	}
        int domainId = Integer.valueOf(args[0]).intValue();
        String topicName= args[1];
        int sampleCount = Integer.valueOf(args[2]).intValue();
        int sendInterval = Integer.valueOf(args[3]).intValue();

        publisherMain(domainId,topicName,sampleCount, sendInterval);
    }

    private DataSamplePublisher() {
        super();
    }

    private static void publisherMain(int domainId,String topicName,
    		int sampleCount,int sendInterval) {

        DomainParticipant participant = null;
        Publisher publisher = null;
        Topic topic = null;
        DataSampleDataWriter writer = null;

        try {
            participant = DomainParticipantFactory.TheParticipantFactory.
            create_participant(
                domainId, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                null , StatusKind.STATUS_MASK_NONE);
            if (participant == null) {
                System.err.println("create_participant error\n");
                return;
            }        

            publisher = participant.create_publisher(
                DomainParticipant.PUBLISHER_QOS_DEFAULT, null ,
                StatusKind.STATUS_MASK_NONE);
            if (publisher == null) {
                System.err.println("create_publisher error\n");
                return;
            }                   

            String typeName = DataSampleTypeSupport.get_type_name();
            DataSampleTypeSupport.register_type(participant, typeName);


            topic = participant.create_topic(
                topicName,
                typeName, DomainParticipant.TOPIC_QOS_DEFAULT,
                null , StatusKind.STATUS_MASK_NONE);
            if (topic == null) {
                System.err.println("create_topic error\n");
                return;
            }           

            writer = (DataSampleDataWriter)
            publisher.create_datawriter(
                topic, Publisher.DATAWRITER_QOS_DEFAULT,
                null, StatusKind.STATUS_MASK_NONE);
            if (writer == null) {
                System.err.println("create_datawriter error\n");
                return;
            }           


            /* Create data sample for writing */
            DataSample instance = new DataSample();

            InstanceHandle_t instance_handle = InstanceHandle_t.HANDLE_NIL;
            /* For a data type that has a key, if the same instance is going to be
            written multiple times, initialize the key here
            and register the keyed instance prior to writing */
            //instance_handle = writer.register_instance(instance);


            for (int count = 0;count < sampleCount;++count) {
            	instance.sample_id=count;
            	instance.ts_milisec=System.currentTimeMillis();
                writer.write(instance, instance_handle);
                System.out.println("Sent sample:"+count);
                try {
                    Thread.sleep(sendInterval);
                } catch (InterruptedException ix) {
                    System.err.println("INTERRUPTED");
                    break;
                }
            }


        } finally {

            if(participant != null) {
                participant.delete_contained_entities();
                DomainParticipantFactory.TheParticipantFactory.
                delete_participant(participant);
            }
           
        }
    }
}

