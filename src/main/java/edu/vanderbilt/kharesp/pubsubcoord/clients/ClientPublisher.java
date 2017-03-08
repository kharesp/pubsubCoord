package edu.vanderbilt.kharesp.pubsubcoord.clients;

import com.rti.dds.publication.Publisher;
import com.rti.dds.topic.Topic;
import com.rti.idl.test.DataSample_64B;
import com.rti.idl.test.DataSample_64BTypeSupport;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;

public class ClientPublisher {
	private static CuratorFramework client;
	private static DistributedBarrier barrier;
	private static DistributedBarrier sub_exited_barrier;
	private static String hostName;
	private static String pid;

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
			
			hostName=InetAddress.getLocalHost().getHostName();
			pid=ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
			
			barrier = new DistributedBarrier(client, String.format("/experiment/%s/barriers/pub", runId));
			sub_exited_barrier=new DistributedBarrier(client, String.format("/experiment/%s/barriers/finished", runId));

			
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
			String client_path=String.format("/experiment/%s/pub/%s/%s_%s_%s", runId,hostName,topicName,hostName,pid);
			client.create().forPath(client_path, new byte[0]);
			//wait before all publishers have joined to begin publishing
			barrier.waitOnBarrier();
            //wait for 30 sec for discovery to finish before publishing
            System.out.println("publisher will sleep for 30 sec until discovery finishes");
            Thread.sleep(30000);
            System.out.println("Publisher will start sending data");
			for (int count = 0; count < sampleCount; ++count) {
				instance.sample_id = count;
				instance.ts_milisec = System.currentTimeMillis();
				datawriter.write(instance);
				if(count%500==0){
					System.out.println("Sent sample:" + count);
				}
				try {
					Thread.sleep(sendInterval);
				} catch (InterruptedException ix) {
					System.err.println("INTERRUPTED");
					break;
				}
			}
			System.out.println("Publisher sent all samples");
			//Don't exit until all subscribers have exited
			sub_exited_barrier.waitOnBarrier();
			System.out.println("All subscribers have exited. Exiting..");
			//Delete this publisher's znode under zk
			client.delete().forPath(client_path);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		} finally {
			participant.shutdown();
		}
	}

}
