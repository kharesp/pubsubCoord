package edu.vanderbilt.kharesp.pubsubcoord.clients;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import com.rti.idl.test.DataSample_64B;
import com.rti.idl.test.DataSample_64BTypeSupport;

public class ClientSubscriber {
	private static String latencyFile;
	private static PrintWriter writer;
	private static int receiveCount=0;
	
	public static void main(String[] args) {
        if(args.length < 6) {
        	System.out.println("Usage: ClientSubscriber domainId topicName typeName sampleCount outdir runId");
        	return;
        }
        int domainId = Integer.valueOf(args[0]).intValue();
        String topicName = args[1];
        String typeName= args[2];
        int sampleCount = Integer.valueOf(args[3]).intValue();
        String outdir=args[4];
        String runId=args[5];
        
        try {
			String file_name= topicName+"_"+
					InetAddress.getLocalHost().getHostAddress()+"_"+
					ManagementFactory.getRuntimeMXBean().getName().split("@")[0]+".csv";
			new File(outdir+"/"+runId).mkdirs();
			latencyFile= outdir+"/"+runId+"/"+file_name;
			writer= new PrintWriter(latencyFile,"UTF-8");
		} catch (UnknownHostException | FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
        
        if (typeName.equals("DataSample_64B")){
    		receive_DataSample_64B(domainId,topicName);
    	}else{
    		System.out.println(String.format("TypeName:%s not recognized.\nExiting..",typeName));
    		return;
    	}
        while(receiveCount<sampleCount){
        	try{
        		Thread.sleep(1000);
        	}catch(InterruptedException e){
        		System.out.println("Interrupted");
        		writer.close();
        		break;
        	}
        }
        writer.close();
    }
    
    public static void receive_DataSample_64B(int domainId,String topicName){
    	GenericSubscriber<DataSample_64B> subscriber=null;
    	try{
    		subscriber= new GenericSubscriber<DataSample_64B>(domainId,
    				topicName,DataSample_64BTypeSupport.get_instance()){

						@Override
						public void process(DataSample_64B sample) {
							receiveCount+=1;
                            long reception_ts=System.currentTimeMillis();
                            System.out.format("Received sample:%d at ts:%d. ts at which sample was sent:%d\n",
                            		sample.sample_id,reception_ts,sample.ts_milisec );
                            long latency= reception_ts-sample.ts_milisec;
                            writer.write(String.format("%d,%d\n",sample.sample_id,latency));
						}
    		};
    		
    	}catch(Exception e){
    		System.out.println(e.getMessage());
    		
    	}finally{
    		subscriber.cleanup();
    	}
    }

}
