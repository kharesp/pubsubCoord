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
import com.rti.idl.test.DataSample_64B;
import com.rti.idl.test.DataSample_64BTypeSupport;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.curator.utils.CloseableUtils;

public class ClientSubscriber {
	private static String latencyFile;
	private static PrintWriter writer;
	private static int receiveCount = 0;
	private static CuratorFramework client;

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

		client = CuratorFrameworkFactory.newClient(zkConnector, new ExponentialBackoffRetry(1000, 3));
		client.start();

		try {
			String file_name = topicName + "_" + InetAddress.getLocalHost().getHostAddress() + "_"
					+ ManagementFactory.getRuntimeMXBean().getName().split("@")[0] + ".csv";
			new File(outdir + "/" + runId).mkdirs();
			latencyFile = outdir + "/" + runId + "/" + file_name;
			writer = new PrintWriter(latencyFile, "UTF-8");

			if (typeName.equals("DataSample_64B")) {
				receive_DataSample_64B(domainId, topicName, sampleCount,runId);
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

	public static void receive_DataSample_64B(int domainId, String topicName, int sampleCount,String runId) {
		DefaultParticipant participant = null;
		try {
			participant = new DefaultParticipant(domainId);
			participant.registerType(DataSample_64BTypeSupport.get_instance());
			Topic topic=participant.create_topic(topicName, DataSample_64BTypeSupport.getInstance());
			Subscriber subscriber = participant.get_default_subscriber();
			GenericDataReader<DataSample_64B> datareader = new GenericDataReader<DataSample_64B>(subscriber,
					topic,DataSample_64BTypeSupport.get_instance()) {
				private SimpleDateFormat sdf= new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
				@Override
				public void process(DataSample_64B sample,SampleInfo info) {
					if (sample.sample_id == receiveCount)
						receiveCount += 1;
					long reception_ts = System.currentTimeMillis();
					System.out.format("Received sample:%d at ts:%d. ts at which sample was sent:%d\n", sample.sample_id,
							reception_ts, sample.ts_milisec);
					long latency = reception_ts - sample.ts_milisec;
					writer.write(String.format("%d,%s,%d,%d\n",reception_ts,sdf.format(new Date(reception_ts)),sample.sample_id, latency));
				}
			};
			client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
				.forPath(String.format("/experiment/%s/sub/sub", runId), new byte[0]);
			datareader.receive();
			wait_for_data(sampleCount);

		} catch (Exception e) {
			System.out.println(e.getMessage());

		} finally {
			participant.shutdown();
		}
	}

	public static void wait_for_data(int sampleCount) {
		while (receiveCount < (sampleCount - 1)) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println("Interrupted");
				writer.close();
				break;
			}
		}
		writer.close();
	}
}
