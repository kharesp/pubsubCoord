package edu.vanderbilt.kharesp.pubsubcoord.routing;


import com.rti.dds.infrastructure.Copyable;
import com.rti.dds.publication.DataWriterQos;
import com.rti.dds.publication.Publisher;
import com.rti.dds.subscription.DataReaderQos;
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
			DataReaderQos readerQos= new DataReaderQos();
			subscriber.get_default_datareader_qos(readerQos);
			readerQos.user_data.value.addAllByte(RoutingService.TOPIC_ROUTE_STRING_CODE.getBytes());

			publisher= secondParticipant.get_default_publisher();
			DataWriterQos writerQos= new DataWriterQos();
			publisher.get_default_datawriter_qos(writerQos);
			writerQos.user_data.value.addAllByte(RoutingService.TOPIC_ROUTE_STRING_CODE.getBytes());

			dw= new GenericDataWriter<T>(publisher,t2,writerQos);
			dr= new GenericDataReader<T>(subscriber,t1,typeSupport,readerQos){
				@Override
				public void process(T sample, SampleInfo info) {
					dw.write(sample);
				}
			};
			dr.receive();
			
		}else if(session_type.equals(PUBLICATION_SESSION)){
			publisher=firstParticipant.get_default_publisher();
			DataWriterQos writerQos= new DataWriterQos();
			publisher.get_default_datawriter_qos(writerQos);
			writerQos.user_data.value.addAllByte(RoutingService.TOPIC_ROUTE_STRING_CODE.getBytes());

			subscriber=secondParticipant.get_default_subscriber();
			DataReaderQos readerQos= new DataReaderQos();
			subscriber.get_default_datareader_qos(readerQos);
			readerQos.user_data.value.addAllByte(RoutingService.TOPIC_ROUTE_STRING_CODE.getBytes());

			dw= new GenericDataWriter<T>(publisher,t1,writerQos);
			dr= new GenericDataReader<T>(subscriber,t2,typeSupport,readerQos){
				@Override
				public void process(T sample, SampleInfo info) {
					dw.write(sample);
				}
			};
			dr.receive();
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
