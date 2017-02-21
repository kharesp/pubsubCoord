package edu.vanderbilt.kharesp.pubsubcoord.clients;

import com.rti.dds.publication.Publisher;
import com.rti.dds.topic.Topic;
import com.rti.idl.test.DataSample_64B;
import com.rti.idl.test.DataSample_64BTypeSupport;
import org.apache.zookeeper.CreateMode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;

public class ClientPublisher {
	private static CuratorFramework client;
	private static DistributedBarrier barrier;

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

		client = CuratorFrameworkFactory.newClient(zkConnector, new ExponentialBackoffRetry(1000, 3));
		client.start();
		try {
			
			barrier = new DistributedBarrier(client, String.format("/experiment/%s/barriers/pub", runId));

			
			if (typeName.equals("DataSample_64B")) {
				publish_DataSample_64B(domainId, topicName, sampleCount, sendInterval,runId);
			} else {
				System.out.println(String.format("TypeName:%s not recognized.\nExiting..", typeName));
				return;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseableUtils.closeQuietly(client);
		}

	}

	public static void publish_DataSample_64B(int domainId, String topicName, int sampleCount, int sendInterval,String runId) {

		DefaultParticipant participant = null;
		DataSample_64B instance = new DataSample_64B();

		try {
			participant = new DefaultParticipant(domainId);
			participant.registerType(DataSample_64BTypeSupport.get_instance());
			Topic topic=participant.create_topic(topicName, DataSample_64BTypeSupport.get_instance());
			Publisher publisher = participant.get_default_publisher();
			GenericDataWriter<DataSample_64B> datawriter = new GenericDataWriter<DataSample_64B>(publisher,topic); 
			client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
				.forPath(String.format("/experiment/%s/pub/pub", runId), new byte[0]);
			//wait before all publishers have joined to begin publishing
			barrier.waitOnBarrier();
			for (int count = 0; count < sampleCount; ++count) {
				instance.sample_id = count;
				instance.ts_milisec = System.currentTimeMillis();
				datawriter.write(instance);

				System.out.println("Sent sample:" + count);
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
