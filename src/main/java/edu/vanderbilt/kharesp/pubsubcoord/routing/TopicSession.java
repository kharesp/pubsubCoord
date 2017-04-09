package edu.vanderbilt.kharesp.pubsubcoord.routing;


import java.io.File;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.Arrays;
import org.apache.log4j.Logger;
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
	
	private static final String LOG_DIR="/home/ubuntu/log/queueing";
	
	private DefaultParticipant firstParticipant;
	private DefaultParticipant secondParticipant;
	public Publisher publisher;
	public Subscriber subscriber;
	private GenericDataWriter<T> dw;
	private GenericDataReader<T> dr;

	public Topic t1;
	public Topic t2;
	public ContentFilteredTopic cft;
	public TypeSupportImpl typeSupport;
	
	private Logger logger;
	private String domainRouteName;
	private String sessionName;
	private String sessionType;
	
	private PrintWriter writer;
	

	public TopicSession(String domainRouteName,String session_type,Topic t1,Topic t2,TypeSupportImpl typeSupport,
			DefaultParticipant p1,DefaultParticipant p2) throws Exception{

		this.domainRouteName=domainRouteName;
		this.sessionType=session_type;
		this.t1=t1;
		this.t2=t2;
		sessionName=t1.get_name();
		this.typeSupport=typeSupport;
		this.firstParticipant=p1;
		this.secondParticipant=p2;
		
		String hostName=InetAddress.getLocalHost().getHostName();
		new File(LOG_DIR).mkdirs();
		String file_name = LOG_DIR + "/queueing_delay_" + 
				domainRouteName.substring(0, domainRouteName.indexOf('@'))+ "_" +
				sessionName + "_" + 
				session_type+ "_" +
				hostName + ".csv";
		writer = new PrintWriter(file_name, "UTF-8");
		writer.write("ts,queueing_delay(ms)\n");
		
		logger= Logger.getLogger(this.getClass().getSimpleName());

		//Subscription_session: Subscriber in first participant and publisher in second participant
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
					long reception_ts=(((long)info.reception_timestamp.sec)*1000)
							+ (info.reception_timestamp.nanosec/1000000);
					long take_ts=System.currentTimeMillis();
					long queueing_delay=take_ts-reception_ts;
					writer.write(String.format("%d,%d\n", take_ts, queueing_delay));
					dw.write(sample);
				}
			};
			dr.receive();
			
		}
		//Publication_session: Publisher in first participant and Subscriber in second participant
		else if(session_type.equals(PUBLICATION_SESSION)){
			publisher=firstParticipant.get_default_publisher();
			DataWriterQos writerQos= new DataWriterQos();
			publisher.get_default_datawriter_qos(writerQos);
			writerQos.user_data.value.addAllByte(RoutingService.INFRASTRUCTURE_NODE_IDENTIFIER.getBytes());

			subscriber=secondParticipant.get_default_subscriber();
			DataReaderQos readerQos= new DataReaderQos();
			subscriber.get_default_datareader_qos(readerQos);
			readerQos.user_data.value.addAllByte(RoutingService.INFRASTRUCTURE_NODE_IDENTIFIER.getBytes());

			dw= new GenericDataWriter<T>(publisher,t1,writerQos);
			
			//Content filter topic to filter out messages originating in this region. 
			String params[]={String.valueOf(RoutingService.regionId)};
			cft= secondParticipant.create_contentfilteredtopic("cft", t2,"region_id <> %0",new StringSeq(Arrays.asList(params)));
			dr= new GenericDataReader<T>(subscriber,cft,typeSupport,readerQos){
				@Override
				public void process(T sample, SampleInfo info) {
					long reception_ts=(((long)info.reception_timestamp.sec)*1000)
							+ (info.reception_timestamp.nanosec/1000000);
					long take_ts=System.currentTimeMillis();
					long queueing_delay=take_ts-reception_ts;
					writer.write(String.format("%d,%d\n", take_ts, queueing_delay));
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
		writer.close();
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
	
	public String domainRouteName(){
		return domainRouteName;
	}
		
}
