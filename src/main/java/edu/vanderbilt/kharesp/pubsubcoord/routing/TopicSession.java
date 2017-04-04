package edu.vanderbilt.kharesp.pubsubcoord.routing;


import java.util.Arrays;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.rti.dds.infrastructure.StringSeq;
import com.rti.dds.publication.DataWriterQos;
import com.rti.dds.publication.Publisher;
import com.rti.dds.subscription.DataReaderQos;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.topic.ContentFilteredTopic;
import com.rti.dds.topic.Topic;
import com.rti.dds.topic.TypeSupportImpl;
import com.rti.idl.BaseDataSample;
import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataReader;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataWriter;

public class TopicSession<T extends BaseDataSample>{
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
	public ContentFilteredTopic cft;
	public TypeSupportImpl typeSupport;
	
	private Logger logger;
	private String sessionName;
	
	public TopicSession(String session_type,Topic t1,Topic t2,TypeSupportImpl typeSupport,
			DefaultParticipant p1,DefaultParticipant p2) throws Exception{
		this.sessionType=session_type;
		this.t1=t1;
		this.t2=t2;
		sessionName=t1.get_name();
		this.typeSupport=typeSupport;
		this.firstParticipant=p1;
		this.secondParticipant=p2;
		
		logger= Logger.getLogger(this.getClass().getSimpleName());
		PropertyConfigurator.configure("log4j.properties");

		if(session_type.equals(SUBSCRIPTION_SESSION)){
			subscriber= firstParticipant.get_default_subscriber();
			DataReaderQos readerQos= new DataReaderQos();
			subscriber.get_default_datareader_qos(readerQos);
			readerQos.user_data.value.addAllByte(RoutingService.INFRASTRUCTURE_NODE_IDENTIFIER.getBytes());

			publisher= secondParticipant.get_default_publisher();
			DataWriterQos writerQos= new DataWriterQos();
			publisher.get_default_datawriter_qos(writerQos);
			writerQos.user_data.value.addAllByte(RoutingService.INFRASTRUCTURE_NODE_IDENTIFIER.getBytes());

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
			writerQos.user_data.value.addAllByte(RoutingService.INFRASTRUCTURE_NODE_IDENTIFIER.getBytes());

			subscriber=secondParticipant.get_default_subscriber();
			DataReaderQos readerQos= new DataReaderQos();
			subscriber.get_default_datareader_qos(readerQos);
			readerQos.user_data.value.addAllByte(RoutingService.INFRASTRUCTURE_NODE_IDENTIFIER.getBytes());

			dw= new GenericDataWriter<T>(publisher,t1,writerQos);
			String params[]={String.valueOf(RoutingService.regionId)};
			cft= secondParticipant.create_contentfilteredtopic("cft", t2,"region_id <> %0",new StringSeq(Arrays.asList(params)));
			dr= new GenericDataReader<T>(subscriber,cft,typeSupport,readerQos){
				@Override
				public void process(T sample, SampleInfo info) {
					dw.write(sample);
				}
			};
			dr.receive();
		}else{
			logger.error(String.format("Session type:%s for topic session:%s not recognized",session_type,sessionName));
			throw new Exception(String.format("Session type:%s for topic session:%s not recognized",session_type,sessionName));
		}
	}

	public void deleteEndpoints() throws Exception{
		if(sessionType.equals(SUBSCRIPTION_SESSION)){
			dr.delete_datareader();
			dw.delete_datawriter();
			firstParticipant.delete_subscriber(subscriber);
			secondParticipant.delete_publisher(publisher);
			
		}else if(sessionType.equals(PUBLICATION_SESSION)){
			dr.delete_datareader();
			dw.delete_datawriter();
			firstParticipant.delete_publisher(publisher);
			secondParticipant.delete_subscriber(subscriber);
		}else{
			logger.error(String.format("Error in deleting endpoints. Session type:%s for topic session:%s not recognized\n",
					sessionType,sessionName));
			
		}
		logger.debug(String.format("Deleted DR,DW,Publisher and Subscriber for session:%s and type:%s", sessionName,sessionType));
		
	}
		
}