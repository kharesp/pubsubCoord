package edu.vanderbilt.kharesp.pubsubcoord.routing;


import com.rti.dds.infrastructure.Copyable;
import com.rti.dds.publication.Publisher;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.topic.Topic;
import com.rti.dds.topic.TypeSupportImpl;

import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataReader;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataWriter;

public class TopicSession<T extends Copyable>{
	public static final String SUBSCRIPTION_SESSION = "subscription_session";
	public static final String PUBLICATION_SESSION = "publication_session";
	
	private DefaultParticipant firstParticipant;
	private DefaultParticipant secondParticipant;
	private GenericDataWriter<T> dw;
	@SuppressWarnings("unused")
	private GenericDataReader<T> dr;
	private String sessionType;

	public Publisher publisher;
	public Subscriber subscriber;
	public Topic t1;
	public Topic t2;
	public TypeSupportImpl typeSupport;

	public TopicSession(String session_type,Topic t1,Topic t2,TypeSupportImpl typeSupport,
			DefaultParticipant p1,DefaultParticipant p2) throws Exception{
		this.sessionType=session_type;
		this.t1=t1;
		this.t2=t2;
		this.typeSupport=typeSupport;
		this.firstParticipant=p1;
		this.secondParticipant=p2;
		if(session_type.equals(SUBSCRIPTION_SESSION)){
			subscriber= firstParticipant.get_default_subscriber();
			publisher= secondParticipant.get_default_publisher();
			dw= new GenericDataWriter<T>(publisher,t2);
			dr= new GenericDataReader<T>(subscriber,t1,typeSupport){
				@Override
				public void process(T sample, SampleInfo info) {
					dw.write(sample);
				}
			};
			
		}else if(session_type.equals(PUBLICATION_SESSION)){
			publisher=firstParticipant.get_default_publisher();
			subscriber=secondParticipant.get_default_subscriber();
			dw= new GenericDataWriter<T>(publisher,t1);
			dr= new GenericDataReader<T>(subscriber,t2,typeSupport){
				@Override
				public void process(T sample, SampleInfo info) {
					dw.write(sample);
				}
			};
		}else{
			throw new Exception(String.format("session_type:%s not recognized",session_type));
		}
	}
	public void deleteEndpoints(){
		if(sessionType.equals(SUBSCRIPTION_SESSION)){
			firstParticipant.delete_subscriber(subscriber);
			secondParticipant.delete_publisher(publisher);
			
		}else if(sessionType.equals(PUBLICATION_SESSION)){
			firstParticipant.delete_publisher(publisher);
			secondParticipant.delete_subscriber(subscriber);
		}
		
	}
		
}
