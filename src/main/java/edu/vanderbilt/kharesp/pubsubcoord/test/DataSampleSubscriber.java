package edu.vanderbilt.kharesp.pubsubcoord.test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import com.rti.dds.domain.*;
import com.rti.dds.infrastructure.*;
import com.rti.dds.subscription.*;
import com.rti.dds.topic.*;


public class DataSampleSubscriber {
	private String latencyFile;
	private PrintWriter writer;
	private int receiveCount;
	private int domainId;
	private int sampleCount;
	private String topicName;

    public static void main(String[] args) {
        if(args.length < 4) {
        	System.out.println("Usage: DataSampleSubscriber domainId topicName sampleCount outdir");
        	return;
        }
        int domainId = Integer.valueOf(args[0]).intValue();
        String topicName = args[1];
        int sampleCount = Integer.valueOf(args[2]).intValue();
        String outdir=args[3];
        new DataSampleSubscriber(domainId,topicName,sampleCount,outdir).subscriberMain();
    }

    public DataSampleSubscriber(int domainId,String topicName,int sampleCount,String outdir) {
    	receiveCount=0;
    	this.domainId=domainId;
    	this.topicName=topicName;
    	this.sampleCount=sampleCount;
    	try {
			latencyFile= topicName+"_"+
					InetAddress.getLocalHost().getHostAddress()+"_"+
					ManagementFactory.getRuntimeMXBean().getName().split("@")[0]+".csv";
			writer= new PrintWriter(outdir+"/"+latencyFile,"UTF-8");
		} catch (UnknownHostException | FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    }

    private void subscriberMain() {
        DomainParticipant participant = null;
        Subscriber subscriber = null;
        Topic topic = null;
        DataReaderListener listener = null;
        DataSampleDataReader reader = null;

        try {
            participant = DomainParticipantFactory.TheParticipantFactory.create_participant(
                domainId, DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT,
                null, StatusKind.STATUS_MASK_NONE);
            if (participant == null) {
                System.err.println("create_participant error\n");
                return;
            }                         

            subscriber = participant.create_subscriber(
                DomainParticipant.SUBSCRIBER_QOS_DEFAULT, null,
                StatusKind.STATUS_MASK_NONE);
            if (subscriber == null) {
                System.err.println("create_subscriber error\n");
                return;
            }     

            String typeName = DataSampleTypeSupport.get_type_name(); 
            DataSampleTypeSupport.register_type(participant, typeName);

            topic = participant.create_topic(
                topicName,
                typeName, DomainParticipant.TOPIC_QOS_DEFAULT,
                null, StatusKind.STATUS_MASK_NONE);
            if (topic == null) {
                System.err.println("create_topic error\n");
                return;
            }                     


            listener = new DataSampleListener();

            reader = (DataSampleDataReader)
            subscriber.create_datareader(
                topic, Subscriber.DATAREADER_QOS_DEFAULT, listener,
                StatusKind.STATUS_MASK_ALL);
            if (reader == null) {
                System.err.println("create_datareader error\n");
                return;
            }                         

            while(receiveCount < sampleCount) {
                try {
                    Thread.sleep(1000); 
                } catch (InterruptedException ix) {
                    System.err.println("INTERRUPTED");
                    break;
                }
            }
            writer.close();
        } finally {

            if(participant != null) {
                participant.delete_contained_entities();
                DomainParticipantFactory.TheParticipantFactory.
                delete_participant(participant);
            }
            
        }
    }

    private class DataSampleListener extends DataReaderAdapter {
        DataSampleSeq _dataSeq = new DataSampleSeq();
        SampleInfoSeq _infoSeq = new SampleInfoSeq();

        public void on_data_available(DataReader reader) {
            DataSampleDataReader DataSampleReader =
            (DataSampleDataReader)reader;

            try {
                DataSampleReader.take(
                    _dataSeq, _infoSeq,
                    ResourceLimitsQosPolicy.LENGTH_UNLIMITED,
                    SampleStateKind.ANY_SAMPLE_STATE,
                    ViewStateKind.ANY_VIEW_STATE,
                    InstanceStateKind.ANY_INSTANCE_STATE);

                for(int i = 0; i < _dataSeq.size(); ++i) {
                    SampleInfo info = (SampleInfo)_infoSeq.get(i);
                    if (info.valid_data) {
                            DataSample sample=(DataSample)_dataSeq.get(i);
                    	    receiveCount+=1;
                            long reception_ts=System.currentTimeMillis();
                            System.out.format("Received sample:%d at ts:%d. ts at which sample was sent:%d\n",
                            		sample.sample_id,reception_ts,sample.ts_milisec );
                            long latency= reception_ts-sample.ts_milisec;
                            writer.write(String.format("%d,%d\n",sample.sample_id,latency));
                    }
                }
            } catch (RETCODE_NO_DATA noData) {
            } finally {
                DataSampleReader.return_loan(_dataSeq, _infoSeq);
            }
        }
    }
}

