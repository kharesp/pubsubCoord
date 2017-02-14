package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.lang.reflect.Method;

import com.rti.dds.infrastructure.Copyable;
import com.rti.dds.publication.Publisher;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.topic.TypeSupportImpl;

import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataReader;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataWriter;

public class TopicSession<T extends Copyable>{
	public static final String SUBSCRIPTION_SESSION = "subscription_session";
	public static final String PUBLICATION_SESSION = "publication_session";
	private static final String TYPES_PACKAGE = "com.rti.idl.test";
	
	@SuppressWarnings("unused")
	private DefaultParticipant firstParticipant;
	private DefaultParticipant secondParticipant;
	private Publisher publisher;
	private Subscriber subscriber;
	
	private String topicName;
	private String typeName;

	public TopicSession(String topicName,String typeName,
			TypeSupportImpl typeSupport,
			Publisher publisher,Subscriber subscriber) throws Exception{

		
try{
			
			if (session_type.equals(SUBSCRIPTION_SESSION)) {
				// subscriber in first_participant and publisher in
				// second_participant
				subscriber = firstParticipant.get_default_subscriber();
				publisher = secondParticipant.get_default_publisher();

			} else if (session_type.equals(PUBLICATION_SESSION)) {
				// publisher in first_participant and subscriber in
				// second_participant
				publisher = firstParticipant.get_default_publisher();
				subscriber = secondParticipant.get_default_subscriber();
			} else {
				System.out.println("Session type not recognized");
				return;
			}
			TypeSupportImpl typeSupport= get_type_support_instance(type_name);
			@SuppressWarnings("rawtypes")
			TopicSession<?> session = new TopicSession(topic_name,type_name,session_type);
			topic_sessions.put(topic_name, session);
			
		}catch(Exception e){
			
		}
		this.topicName=topicName;
		this.typeName=typeName;
		this.publisher=publisher;
		this.subscriber=subscriber;
		dataWriter=new GenericDataWriter<T>(publisher,topicName,typeSupport);
		dataReader= new GenericDataReader<T>(subscriber,topicName,typeSupport){
			@Override
			public void process(T sample,SampleInfo info) {
				dataWriter.write(sample);
				
			}
		};
	}
	
	public void delete(){
		//delete Topic
		
		//unregister Type
		publisher.get_participant().unregister_type(typeName);
		subscriber.get_participant().unregister_type(typeName);
		
		//Delete DW and DR
		publisher.delete_contained_entities();
		subscriber.delete_contained_entities();
	}
	
	public String getName(){
		return topicName;
	}
	
	private TypeSupportImpl get_type_support_instance(String type_name) {
		try {
			Class<?> type_support_class = Class.forName(TYPES_PACKAGE + "." + type_name);
			Method getInstance = type_support_class.getMethod("get_instance");
			return (TypeSupportImpl) getInstance.invoke(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
}
