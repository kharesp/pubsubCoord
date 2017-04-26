package edu.vanderbilt.kharesp.pubsubcoord.clients;

import java.io.File;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.topic.Topic;
import com.rti.idl.DataSample_64B;
import com.rti.idl.DataSample_64BTypeSupport;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ClientSubscriber {

	private static Logger logger;

	private static CuratorFramework client;

	private static String hostName;
	private static String region;
	private static String pid;
	
	private static String latencyFile;
	private static PrintWriter writer;
	private static int receiveCount = 0;

	public static void main(String[] args) {
		if (args.length < 7) {
			System.out.println(
					"Usage: ClientSubscriber domainId topicName typeName sampleCount outdir runId zkConnector");
			return;
		}
		int domainId = Integer.valueOf(args[0]).intValue();
		String topicName = args[1];
		String typeName = args[2];
		int sampleCount = Integer.valueOf(args[3]).intValue();
		String outdir = args[4];
		String runId = args[5];
		String zkConnector = args[6];
		
		logger=  Logger.getLogger(ClientSubscriber.class.getSimpleName());
		PropertyConfigurator.configure("log4j.properties");

		client = CuratorFrameworkFactory.newClient(zkConnector, new ExponentialBackoffRetry(1000, 3));
		client.start();

		try {
			hostName=InetAddress.getLocalHost().getHostName();
			region=hostName.substring(hostName.indexOf('i')+1, hostName.indexOf('-'));
			pid=ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

			String file_name = topicName + "_" + 
					hostName + "_"+
					pid + ".csv";
			new File(outdir + "/" + runId).mkdirs();
			latencyFile = outdir + "/" + runId + "/" + file_name;
			writer = new PrintWriter(latencyFile, "UTF-8");
			writer.write("reception ts,recepiton date,reception time,sample id,sender ts,latency(ms),source ts,reception delay(ms),interarrival time(ms)\n");

			if (typeName.equals("DataSample_64B")) {
				receive_DataSample_64B(domainId, topicName, sampleCount,runId);
			} else {
				logger.error(String.format("TypeName:%s not recognized.\nExiting..", typeName));
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseableUtils.closeQuietly(client);
		}

	}

	public static void receive_DataSample_64B(int domainId, String topicName, int sampleCount,String runId) {
		DefaultParticipant participant = null;
		logger.debug(String.format("Starting subscriber for topic:%s in domainId:%d", topicName,domainId));
		try {
			//initialize DDS entities
			participant = new DefaultParticipant(domainId);
			participant.registerType(DataSample_64BTypeSupport.get_instance());
			Topic topic=participant.create_topic(topicName, DataSample_64BTypeSupport.getInstance());
			Subscriber subscriber = participant.get_default_subscriber();
			GenericDataReader<DataSample_64B> datareader = new GenericDataReader<DataSample_64B>(subscriber,
					topic,DataSample_64BTypeSupport.get_instance()) {
				private SimpleDateFormat sdf= new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
				private long prev_recv_ts=-1;
				@Override
				public void process(DataSample_64B sample,SampleInfo info) {
						receiveCount += 1;
						long source_ts = (((long) info.source_timestamp.sec) * 1000)
								+ (info.source_timestamp.nanosec / 1000000);
						long reception_ts = (((long) info.reception_timestamp.sec) * 1000)
								+ (info.reception_timestamp.nanosec / 1000000);
						long reception_delay = reception_ts - source_ts;

						long interarrival_time = ((prev_recv_ts == -1) ? 0 : (reception_ts - prev_recv_ts));
						prev_recv_ts = reception_ts;
						long latency = Math.abs(reception_ts - sample.ts_milisec);

						logger.debug(String.format("Received sample:%d, sample.run_id:%d, reception_ts:%d,"
								+ " sender_ts:%d, latency:%d, source_ts:%d, delay:%d\n",
								sample.sample_id, sample.run_id,reception_ts, sample.ts_milisec,latency, source_ts,reception_delay));

						writer.write(String.format("%d,%s,%d,%d,%d,%d,%d,%d\n", reception_ts,
								sdf.format(new Date(reception_ts)), sample.sample_id, sample.ts_milisec, latency,
								source_ts, reception_delay, interarrival_time));
				}
			};
			
			logger.debug("Initialized DDS entities");
			
			//create znode for this subscriber
			String client_path=String.format("/experiment/%s/sub/region_%s/%s/%s_%s_%s", runId,region,hostName,topicName,hostName,pid);
			client.create().creatingParentsIfNeeded().forPath(client_path, new byte[0]);
			
			logger.debug(String.format("Created znode for this subscriber at:%s",client_path));
			
			//start listening for published data
			datareader.receive();
			
			//wait until all samples have been received
			wait_for_data(sampleCount);
			
			//delete znode for this subscriber
			client.delete().forPath(client_path);
			
			logger.debug(String.format("Deleted znode for this subscriber at:%s", client_path));

		} catch (Exception e) {
			logger.error(e.getMessage(),e);

		} finally {
			try {
				participant.shutdown();
			} catch (Exception e) {
				//ignored
			}
		}
	}

	public static void wait_for_data(int sampleCount) {
		while (receiveCount < (sampleCount - 1)) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error(e.getMessage(),e);
				writer.close();
				break;
			}
		}
		logger.debug(String.format("Subscriber received:%d samples.Exiting..",receiveCount));
		writer.close();
	}
}
