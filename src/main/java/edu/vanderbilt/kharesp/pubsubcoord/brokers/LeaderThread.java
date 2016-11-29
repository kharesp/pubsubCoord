package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.util.HashSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class LeaderThread implements Runnable {

	private final LeaderLatch leaderLatch;
	private PathChildrenCache topicChildrenCache;
	private PathChildrenCache rbChildrenCache;
	private CuratorFramework client;
	private Logger logger;
	private String rbAddress;

	LeaderThread(CuratorFramework curatorClient, String rbAddress) {
		//configure logger
		this.rbAddress=rbAddress;
		logger=LogManager.getLogger(this.getClass().getSimpleName());
		client = curatorClient;
		leaderLatch = new LeaderLatch(client, CuratorHelper.LEADER_PATH, rbAddress);
	}

	public void run() {
		try {
			logger.debug(String.format("Starting leader thread for RB:%s\n",rbAddress));
			// Start LeaderLatch
			leaderLatch.start();
			// Block until leadership is acquired
			leaderLatch.await();
			// Do some work if this routing broker is elected as a leader
			logger.debug(String.format("Routing broker:%s becomes the leader\n",rbAddress));
			doWork();
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}

	void doWork() {
		try {
			// Create a cache for topics (to detect events of creation and
			// deletion of topics)
			logger.debug(String.format("Leader thread for RB:%s installing listener for topics path:%s\n",
					rbAddress,CuratorHelper.TOPIC_PATH));
			topicChildrenCache = new PathChildrenCache(client, CuratorHelper.TOPIC_PATH, true);
			topicChildrenCache.start();
			addTopicChildrenListener(topicChildrenCache);

			// Create a cache for routing brokers (to detect events of creation
			// and deletion of rb)
			logger.debug(String.format("Leader thread for RB:%s installing listener for RoutingBroker path:%s\n",
					rbAddress,CuratorHelper.ROUTING_BROKER_PATH));
			rbChildrenCache = new PathChildrenCache(client, CuratorHelper.ROUTING_BROKER_PATH, true);
			rbChildrenCache.start();
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}

	void addTopicChildrenListener(PathChildrenCache cache) {
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				switch (event.getType()) {
				// When a new topic is created
				case CHILD_ADDED: {
					
					String topic_path=event.getData().getPath();
					String topic=topic_path.split("/")[2];

					logger.debug(String.format("Topic:%s created\n",topic));

					// Update this rbChildrenCache (Synchronizing) to get the updated list of RBs
					rbChildrenCache.rebuild();

					// Find the least loaded routing broker
					int min_value = Integer.MAX_VALUE,min_index=0;

					for (int i = 0; i < rbChildrenCache.getCurrentData().size(); i++) {
						ChildData rbNode = rbChildrenCache.getCurrentData().get(i);
						@SuppressWarnings("unchecked")
						HashSet<String> rbNodeData = (HashSet<String>) CuratorHelper.deserialize(rbNode.getData());
						if (rbNodeData.size() < min_value) {
							min_index = i;
							min_value = rbNodeData.size();
						}
						logger.debug(String.format("RB:%s has %d topics\n",rbNode.getPath(),rbNodeData.size()));
					}

					//least loaded RB
					ChildData rbData = rbChildrenCache.getCurrentData().get(min_index);
					logger.debug(String.format("New topic:%s will be assigned to least loaded RB:%s\n",topic,
							rbData.getPath()));

					//assign the new topic to the least loaded RB
					@SuppressWarnings("unchecked")
					HashSet<String> topicSet = (HashSet<String>) CuratorHelper.deserialize(rbData.getData());
					topicSet.add(topic);
					client.setData().forPath(rbData.getPath(), CuratorHelper.serialize(topicSet));
					
					logger.debug(String.format("RB:%s locator assigned to topic:%s node\n",
							rbData.getPath(),topic));
					//Set topic node's data to chosen RB's address
					client.setData().forPath(topic_path, rbData.getPath().split("/")[2].getBytes());

					break;
				}
				case CHILD_REMOVED: {
					break;
				}
				default:
					break;
				}
			}
		});
	}
}
