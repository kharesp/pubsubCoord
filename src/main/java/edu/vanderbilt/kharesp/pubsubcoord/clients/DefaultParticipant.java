package edu.vanderbilt.kharesp.pubsubcoord.clients;

import com.rti.dds.domain.DomainParticipant;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.publication.Publisher;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.topic.Topic;
import com.rti.dds.topic.TypeSupportImpl;

public class DefaultParticipant {
    private int domainId;
    private DomainParticipant participant;
    
    public DefaultParticipant(int domainId) throws Exception{
    	this.domainId=domainId;
    	participant=DomainParticipantFactory.
    			TheParticipantFactory.
                create_participant(this.domainId,
                		DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                        null , StatusKind.STATUS_MASK_NONE);
        if (participant == null) {
        	throw new Exception("create_participant error\n");
        }        
    }
    
    public Publisher get_default_publisher() throws Exception{
    	Publisher publisher=participant.create_publisher(
				DomainParticipant.PUBLISHER_QOS_DEFAULT,
				null,
				StatusKind.STATUS_MASK_NONE);
		if (publisher == null) {
			throw new Exception("create_publisher error\n");
		}	
		return publisher;
    }
    
    public Subscriber get_default_subscriber() throws Exception{
		Subscriber subscriber = participant.create_subscriber(
				DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null,
				StatusKind.STATUS_MASK_NONE);
		if (subscriber == null) {
			throw new Exception("create_subscriber error\n");
		}
		return subscriber;
    }
    
    public Topic create_topic(String topicName,TypeSupportImpl typeSupport) throws Exception{
    	Topic topic=participant.create_topic(topicName,typeSupport.get_type_nameI(),
				DomainParticipant.TOPIC_QOS_DEFAULT, null,
				StatusKind.STATUS_MASK_NONE);
		if (topic == null) {
			throw new Exception("create_topic error\n");
		}
		return topic;
    }
    
    public void shutdown(){
    	if (participant!=null){
    		participant.delete_contained_entities();
    		DomainParticipantFactory.get_instance().delete_participant(participant);
    	}
    }
    
    public void registerType(TypeSupportImpl typeSupport) throws Exception{
    	typeSupport.register_typeI(participant, typeSupport.get_type_nameI());
    }
    
    public DomainParticipant participant(){
    	return participant;
    }
}
