package edu.vanderbilt.kharesp.pubsubcoord.clients;

import com.rti.dds.publication.Publisher;
import com.rti.dds.topic.Topic;
import com.rti.idl.DataSample_64B;
import com.rti.idl.DataSample_64BTypeSupport;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;

public class ClientPublisher {
	private static CuratorFramework client;
	private static DistributedBarrier barrier;
	private static DistributedBarrier sub_exited_barrier;

	private static String pid;
	private static String hostName;
	private static String region;
	private static int network_id;

	private static Logger logger;

	public static void main(String[] args) {
		if (args.length < 7) {
			System.out.println(
					"Usage: ClientPublisher domainId topicName typeName sampleCount sendInterval runId zkConnector");
			return;
		}
		int domainId = Integer.valueOf(args[0]).intValue();
		String topicName = args[1];
		String typeName = args[2];
		int sampleCount = Integer.valueOf(args[3]).intValue();
		int sendInterval = Integer.valueOf(args[4]).intValue();
		String runId = args[5];
		String zkConnector = args[6];
		
		logger=  Logger.getLogger(ClientPublisher.class.getSimpleName());
		PropertyConfigurator.configure("log4j.properties");

		client = CuratorFrameworkFactory.newClient(zkConnector, new ExponentialBackoffRetry(1000, 3));
		client.start();
		try {
			
			//network_id will be used to set the region_id filed of the data sample to identify publisher's network
			String address= InetAddress.getLocalHost().getHostAddress();
			network_id=Integer.parseInt(address.split("\\.")[2]);
			
			//hostname,logical region id & processId of this subscriber will be used for creating its znode
			hostName=InetAddress.getLocalHost().getHostName();
			region=hostName.substring(hostName.indexOf('i')+1, hostName.indexOf('-'));
			pid=ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
			
			barrier = new DistributedBarrier(client, String.format("/experiment/%s/barriers/pub", runId));
			sub_exited_barrier=new DistributedBarrier(client, String.format("/experiment/%s/barriers/finished", runId));

			if (typeName.equals("DataSample_64B")) {
				publish_DataSample_64B(domainId, topicName, sampleCount, sendInterval,runId);
			} else {
				logger.error(String.format("TypeName:%s not recognized.\nExiting..", typeName));
				return;
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		} finally {
			CloseableUtils.closeQuietly(client);
		}

	}

	public static void publish_DataSample_64B(int domainId, String topicName,
			int sampleCount, int sendInterval,String runId) {

		logger.debug(String.format("Starting publisher for topic:%s in domainId:%d",
				topicName,domainId));
		DefaultParticipant participant = null;
		DataSample_64B instance = new DataSample_64B();

		try {
			//initialize DDS entities
			participant = new DefaultParticipant(domainId);
			participant.registerType(DataSample_64BTypeSupport.get_instance());
			Topic topic=participant.create_topic(topicName, DataSample_64BTypeSupport.get_instance());
			Publisher publisher = participant.get_default_publisher();
			GenericDataWriter<DataSample_64B> datawriter = new GenericDataWriter<DataSample_64B>(publisher,topic); 
			
			logger.debug("Initialized DDS entities");
			
			//create znode for this publisher
			String client_path=String.format("/experiment/%s/pub/region_%s/%s/%s_%s_%s", 
					runId,region,hostName,topicName,hostName,pid);
			client.create().creatingParentsIfNeeded().forPath(client_path, new byte[0]);
			
			logger.debug(String.format("Created znode for this publisher at:%s", client_path));

			//wait before all publishers have joined to begin publishing
			barrier.waitOnBarrier();

			//publish data
            logger.debug("Publisher will start sending data");
			for (int count = 0; count < sampleCount; ++count) {
				instance.sample_id = count;
				//set region_id field to identify this sample's originating network. 
				instance.region_id=network_id;
				instance.ts_milisec = System.currentTimeMillis();

				datawriter.write(instance);

				if(count%500==0){
					logger.debug("Sent sample:" + count);
				}
				try {
					Thread.sleep(sendInterval);
				} catch (InterruptedException ix) {
					logger.error(ix.getMessage(),ix);
					break;
				}
			}
			logger.debug("Publisher sent all samples");

			//Don't exit until all subscribers have exited
			sub_exited_barrier.waitOnBarrier();
			logger.debug("All subscribers have exited. Exiting..");

			//Delete this publisher's znode under zk
			client.delete().forPath(client_path);
			
			logger.debug(String.format("Deleted znode for this publisher at:%s", client_path));
			
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

}