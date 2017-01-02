package edu.vanderbilt.kharesp.pubsubcoord.clients;

import com.rti.dds.publication.Publisher;
import com.rti.idl.test.DataSample_64B;
import com.rti.idl.test.DataSample_64BTypeSupport;

public class ClientPublisher {
	
	public static void main(String[] args) {
    	if (args.length < 5){
    		System.out.println("Usage: ClientPublisher domainId topicName typeName sampleCount sendInterval");
    		return;
    	}
        int domainId = Integer.valueOf(args[0]).intValue();
        String topicName= args[1];
        String typeName= args[2];
        int sampleCount = Integer.valueOf(args[3]).intValue();
        int sendInterval = Integer.valueOf(args[4]).intValue();

    	if (typeName.equals("DataSample_64B")){
    		publish_DataSample_64B(domainId,topicName,sampleCount,sendInterval);
    	}else{
    		System.out.println(String.format("TypeName:%s not recognized.\nExiting..",typeName));
    		return;
    	}
    }	
	
	
	public static void publish_DataSample_64B(int domainId,String topicName,
			int sampleCount,int sendInterval){

		DefaultParticipant participant=null;
		DataSample_64B instance = new DataSample_64B();

		try {
			participant = new DefaultParticipant(domainId);
			Publisher publisher = participant.get_default_publisher();
			GenericDataWriter<DataSample_64B> datawriter = 
					new GenericDataWriter<DataSample_64B>(publisher, topicName,
					DataSample_64BTypeSupport.get_instance());

			for (int count = 0; count < sampleCount; ++count) {
				instance.sample_id = count;
				instance.ts_milisec = System.currentTimeMillis();
				datawriter.write(instance);

				// if (count % 100 == 0) {
				System.out.println("Sent sample:" + count);
				// }
				try {
					Thread.sleep(sendInterval);
				} catch (InterruptedException ix) {
					System.err.println("INTERRUPTED");
					break;
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			participant.shutdown();
		}
	}

}
