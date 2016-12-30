package edu.vanderbilt.kharesp.pubsubcoord.clients;


import java.util.LinkedList;
import com.rti.dds.infrastructure.Copyable;
import com.rti.dds.infrastructure.RETCODE_NO_DATA;
import com.rti.dds.infrastructure.ResourceLimitsQosPolicy;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.dds.subscription.DataReader;
import com.rti.dds.subscription.DataReaderAdapter;
import com.rti.dds.subscription.InstanceStateKind;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.SampleInfoSeq;
import com.rti.dds.subscription.SampleStateKind;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.subscription.ViewStateKind;
import com.rti.dds.topic.Topic;
import com.rti.dds.topic.TypeSupportImpl;
import com.rti.dds.util.LoanableSequence;

public abstract class GenericSubscriber<T extends Copyable> {

	private int domainId;
	private String topicName;
	private TypeSupportImpl typeSupport;
	
	private DefaultParticipant participant;
	private Subscriber subscriber;
	private Topic topic;
	private DataReader reader;
	private SampleInfoSeq infoSeq;
	private LoanableSequence dataSeq;


    public GenericSubscriber(int domainId,String topicName,TypeSupportImpl typeSupport) throws Exception {
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
		subscriber= participant.get_default_subscriber();
		
		topic = participant.create_topic(topicName,typeSupport);
		
		
		DataReaderListener listener= new DataReaderListener();
		
		reader= subscriber.create_datareader(topic,Subscriber.DATAREADER_QOS_DEFAULT,
				listener,StatusKind.STATUS_MASK_ALL);
		if (reader== null) {
			throw new Exception("create_datareader error\n");
		}
		dataSeq= new LoanableSequence(typeSupport.get_type());
		infoSeq = new SampleInfoSeq();
	}
	
	private class DataReaderListener extends DataReaderAdapter{
		public void on_data_available(DataReader reader){
			take().forEach(t-> process(t));
		}
	}
	
	@SuppressWarnings("unchecked") 
	private Iterable<T> take(){
		LinkedList<T> data= new LinkedList<T>();
		try {
			reader.take_untyped(dataSeq, infoSeq, ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
					SampleStateKind.ANY_SAMPLE_STATE, ViewStateKind.ANY_VIEW_STATE,
					InstanceStateKind.ANY_INSTANCE_STATE);

			for (int j = 0; j < dataSeq.size(); ++j) {
				if (((SampleInfo) infoSeq.get(j)).valid_data) {
					T sample= (T) dataSeq.get(j);
					T dataCopy= (T) typeSupport.create_data();
					dataCopy.copy_from(sample);
					data.addLast(dataCopy);
				}
			}
		} catch (RETCODE_NO_DATA noData)
		{
		} finally {
			reader.return_loan_untyped(dataSeq, infoSeq);
		}
		return data;
	}
	
	public void cleanup(){
		participant.shutdown();
	}
	
	public abstract void process(T sample);
   
}
