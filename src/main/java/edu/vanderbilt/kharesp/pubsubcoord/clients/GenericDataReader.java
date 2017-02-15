package edu.vanderbilt.kharesp.pubsubcoord.clients;


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

public abstract class GenericDataReader<T extends Copyable> {

	private TypeSupportImpl typeSupport;
	private Subscriber subscriber;
	private Topic topic;
	private DataReader reader;
	private SampleInfoSeq infoSeq;
	private LoanableSequence dataSeq;


    public GenericDataReader(Subscriber subscriber,Topic topic,TypeSupportImpl typeSupport) throws Exception {
    	this.subscriber=subscriber;
    	this.topic=topic;
    	this.typeSupport=typeSupport;
		initialize();
	}
	
	private void initialize() throws Exception{
		reader= subscriber.create_datareader(topic,Subscriber.DATAREADER_QOS_DEFAULT,
				null,StatusKind.STATUS_MASK_ALL);
		if (reader== null) {
			throw new Exception("create_datareader error\n");
		}
		dataSeq= new LoanableSequence(typeSupport.get_type());
		infoSeq = new SampleInfoSeq();
	}
	
	private class DataReaderListener extends DataReaderAdapter{
		public void on_data_available(DataReader reader){
			take();
		}
	}
	
	public void receive(){
		DataReaderListener listener= new DataReaderListener();
		reader.set_listener(listener, StatusKind.STATUS_MASK_ALL);
	}
	
	@SuppressWarnings("unchecked") 
	private void  take(){
		try {
			reader.take_untyped(dataSeq, infoSeq, ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
					SampleStateKind.ANY_SAMPLE_STATE, ViewStateKind.ANY_VIEW_STATE,
					InstanceStateKind.ANY_INSTANCE_STATE);

			for (int j = 0; j < dataSeq.size(); ++j) {
				SampleInfo info=(SampleInfo)infoSeq.get(j);
				if (info.valid_data) {
					T sample= (T) dataSeq.get(j);
					T dataCopy= (T) typeSupport.create_data();
					dataCopy.copy_from(sample);
					process(dataCopy,info);
				}
			}
		} catch (RETCODE_NO_DATA noData)
		{
		} finally {
			reader.return_loan_untyped(dataSeq, infoSeq);
		}
	}
	
	public abstract void process(T sample,SampleInfo info);
   
}
