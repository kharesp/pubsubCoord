package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class CuratorHelper {
	public static final String ROUTING_BROKER_PATH = "/routingBrokers";
    public static final String LEADER_PATH = "/leader";
    public static final String TOPIC_PATH = "/topics";
    
    private Logger logger;
    private CuratorFramework client;
    
    public CuratorHelper(String zkConnector){
    	logger= LogManager.getLogger(this.getClass().getSimpleName());
    	client= CuratorFrameworkFactory.newClient(zkConnector,
                new ExponentialBackoffRetry(1000, 3));
    	client.start();
    }
    
    public void close()
    {
    	CloseableUtils.closeQuietly(client);
    }
    
    //ensure zk path exists
    public void ensurePathExists(String path){
    	try {
    		if(client.checkExists().forPath(path)==null){
    			client.create().
					creatingParentsIfNeeded().forPath(path);
    			logger.debug(String.format("Created zk path:%s",path));
    		}else{
    			logger.debug(String.format("Zk path:%s already exists", path));
    		}

		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
    }

    //create znode
    public void create(String path,Object data, CreateMode mode){
    	try {
			if(client.checkExists().forPath(path) == null){
				client.create().creatingParentsIfNeeded().
					withMode(mode).forPath(path, serialize(data));
				logger.debug(String.format("Created znode at:%s with mode:%s",path,mode.name()));
			}else{
				logger.debug(String.format("Znode at:%s already exists", path));
			}
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
    }
   
    //get data from znode
    public Object get(String path){
    	try {
			return CuratorHelper.deserialize(client.getData().forPath(path));
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
    	return null;
    }
   
    //delete znode
    public void delete(String path){
    	try {
			client.delete().forPath(path);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
    }
    
    //get children znodes under path
    public List<String> getChildren(String path){
    	try {
			return client.getChildren().forPath(path);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
    	return null;
    }
    
    //create node cache 
    public NodeCache nodeCache(String path){
    	return new NodeCache(client,path);
    }
    
    //create path children cache
    public PathChildrenCache pathChildrenCache(String path, boolean cacheData){
    	return new PathChildrenCache(client,path,cacheData);
    }
    
    //create leader latch
    public LeaderLatch leaderLatch(String path,String id){
    	return new LeaderLatch(client,path,id);
    }
    
    // Serialize object
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    // De-serialize object
    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
    
    public static void closeQuitely(Closeable object){
    	CloseableUtils.closeQuietly(object);
    }
    

}
