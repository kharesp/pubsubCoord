package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import com.rti.dds.infrastructure.RETCODE_NO_DATA;
import com.rti.dds.publication.builtin.PublicationBuiltinTopicData;
import com.rti.dds.publication.builtin.PublicationBuiltinTopicDataDataReader;
import com.rti.dds.subscription.DataReader;
import com.rti.dds.subscription.DataReaderAdapter;
import com.rti.dds.subscription.InstanceStateKind;
import com.rti.dds.subscription.SampleInfo;
import com.rti.idl.RTI.RoutingService.Administration.CommandKind;

public class BuiltinPublisherListener extends DataReaderAdapter {
	private static final String TOPIC_ROUTE_CODE = "107"; 
    private static final String TOPIC_ROUTE_STRING_CODE = "k";
    // public facing ports for interconnecting domains in the cloud 
    private static final String RB_P1_BIND_PORT = "8500";
    // public facing port for sending/receiving data for this local domain
    private static final String EB_P2_BIND_PORT = "8502";
    private static final String DOMAIN_ROUTE_NAME_PREFIX = "EdgeBrokerDomainRoute";

	private String ebAddress;
	private String ebLocator;
	private String domainRouteName;
	private CuratorFramework client;
	private RoutingServiceAdministrator rs;

	private PublicationBuiltinTopicData publication_builtin_topic_data = new PublicationBuiltinTopicData();
	private SampleInfo info= new SampleInfo();
	private HashSet<String> pubPeerList = new HashSet<String>();	
	private HashMap<String, Integer> pubTopicCount= new HashMap<>();
	private HashMap<String, NodeCache> pubTopicNodeCache = new HashMap<String, NodeCache>();

	public BuiltinPublisherListener(String ebAddress,CuratorFramework client, RoutingServiceAdministrator rs){
		this.ebAddress=ebAddress;
		ebLocator=ebAddress+":"+EB_P2_BIND_PORT;
		domainRouteName=DOMAIN_ROUTE_NAME_PREFIX+"@"+ebAddress;
		this.rs=rs;
		this.client=client;
	}

	public void on_data_available(DataReader reader) {
		PublicationBuiltinTopicDataDataReader builtin_reader = (PublicationBuiltinTopicDataDataReader) reader;
		try {
			while (true) {
				builtin_reader.take_next_sample(publication_builtin_topic_data, info);
				if (info.instance_state == InstanceStateKind.ALIVE_INSTANCE_STATE) {
					System.out.println("Built-in Reader: found publisher \n\tparticipant_key->"
							+ Arrays.toString(publication_builtin_topic_data.participant_key.value) + "\n\tkey->"
							+ Arrays.toString(publication_builtin_topic_data.key.value) + "\n\ttopic_name->"
							+ publication_builtin_topic_data.topic_name);
					add_dw();
				}
				if (info.instance_state == InstanceStateKind.NOT_ALIVE_DISPOSED_INSTANCE_STATE
						|| info.instance_state == InstanceStateKind.NOT_ALIVE_NO_WRITERS_INSTANCE_STATE) {
					System.out.println(
							"Built-in Reader: publisher instance state:" + info.instance_state + "\n\tparticipant_key->"
									+ Arrays.toString(publication_builtin_topic_data.participant_key.value)
									+ "\n\tkey->" + Arrays.toString(publication_builtin_topic_data.key.value)
									+ "\n\ttopic_name->" + publication_builtin_topic_data.topic_name);
					delete_dw();
				}

			}
		} catch (RETCODE_NO_DATA noData) {
			return;
		} catch (Exception e) {
		}

	}
	private void add_dw(){
		 String userData =
                 new String(publication_builtin_topic_data.user_data.value.toArrayByte(null));

         // If it is an endpoint created by topic_route, we skip this process
         if (!(userData.equals(TOPIC_ROUTE_STRING_CODE))) {
             // Add this edge broker's locator to UserDataQoS
             publication_builtin_topic_data.user_data.value.addAllByte(ebLocator.getBytes());

             // Create a base path for znode of DRs
             String pubPath = (CuratorHelper.TOPIC_PATH
                     + "/"
                     + publication_builtin_topic_data.topic_name
                     + "/pub").replaceAll("\\s", "");

             // Create a unique znode name for publication
             String pubZnodeName =
                     (Arrays.toString(publication_builtin_topic_data.key.value) + "@" +
                             Arrays.toString(publication_builtin_topic_data.participant_key.value) + "@" +
                             ebAddress).replaceAll("\\s", "");

             // Create znode for publications
            try {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
					 	.forPath(ZKPaths.makePath(pubPath, pubZnodeName),CuratorHelper.serialize(publication_builtin_topic_data));
			} catch (Exception e1) {
				e1.printStackTrace();
			}

            // Create cache and its listener for related topics to subscriptions
            String topicCachePath = (CuratorHelper.TOPIC_PATH + "/" + publication_builtin_topic_data.topic_name).replaceAll("\\s", "");

            // add this dw to pubTopicCount
            if(pubTopicCount.containsKey(topicCachePath)){
                pubTopicCount.put(topicCachePath,pubTopicCount.get(topicCachePath)+1);
            }else{
                pubTopicCount.put(topicCachePath,1);
            }
            System.out.format("Current publisher count for topic: %s is: %d\n",publication_builtin_topic_data.topic_name,
                    pubTopicCount.get(topicCachePath));

            if (!pubTopicNodeCache.containsKey(topicCachePath)) {
                // Create a topic route
                createPubTopicRoute(publication_builtin_topic_data);

                // Create a cache for topics to receive locator of routing broker
                NodeCache topicCache = new NodeCache(client, topicCachePath);

                pubTopicNodeCache.put(topicCachePath, topicCache);
                // Register a listener for cache
                addTopicNodeCacheListener(topicCache);
                // Start cache
                try {
                    topicCache.start();
                }catch(Exception e){
                    System.out.println(e.getMessage());
                }
                }
         	} else {
         		System.out.println("This publication is not for users!");
         	}
	}
	private void delete_dw(){
		
	}
	private void addTopicNodeCacheListener(final NodeCache cache) {
         cache.getListenable().addListener(new NodeCacheListener() {
             @Override
             public void nodeChanged() throws Exception {
                 ChildData currentData = cache.getCurrentData();
                 String rbLocator = "tcpv4_wan://" + new String(currentData.getData()) + ":" + RB_P1_BIND_PORT;
                 if (!pubPeerList.contains(rbLocator)) {
                     pubPeerList.add(rbLocator);
                     rs.addPeer(domainRouteName,rbLocator, false);
                 }
             }
         });
    }
	private void createPubTopicRoute(PublicationBuiltinTopicData data){
		rs.sendRequest(CommandKind.RTI_ROUTING_SERVICE_COMMAND_CREATE, 
				 "str://\"<session name=\"" + data.topic_name + "SubscriptionSession\">" +
                         "<topic_route name=\"" + data.type_name + "SubscriptionRoute\">" +
                         "<route_types>true</route_types>" +
                         "<publish_with_original_info>true</publish_with_original_info>" +
                         "<publish_with_original_timestamp>true</publish_with_original_timestamp>" +
                         "<input participant=\"1\">" +
                         "<topic_name>" + data.topic_name + "</topic_name>" +
                         "<registered_type_name>" + data.type_name + "</registered_type_name>" +
                         "<creation_mode>IMMEDIATE</creation_mode>" +
                         "<datareader_qos>" +
                         "<reliability>" +
                         "<kind>RELIABLE_RELIABILITY_QOS</kind>" +
                         "</reliability>" +
                         "<durability>" +
                         "<kind>TRANSIENT_LOCAL_DURABILITY_QOS</kind>" +
                         "</durability>" +
                         "<history>" +
                         "<kind>KEEP_ALL_HISTORY_QOS</kind>" +
                         "</history>" +
                         "<user_data><value>" + TOPIC_ROUTE_CODE + "</value></user_data>" +
                         "</datareader_qos>" +
                         "</input>" +
                         "<output>" +
                         "<topic_name>" + data.topic_name + "</topic_name>" +
                         "<registered_type_name>" + data.type_name + "</registered_type_name>" +
                         "<creation_mode>IMMEDIATE</creation_mode>" +
                         "<datawriter_qos>" +
                         "<reliability>" +
                         "<kind>RELIABLE_RELIABILITY_QOS</kind>" +
                         "</reliability>" +
                         "<durability>" +
                         "<kind>TRANSIENT_LOCAL_DURABILITY_QOS</kind>" +
                         "</durability>" +
                         "<history>" +
                         "<kind>KEEP_ALL_HISTORY_QOS</kind>" +
                         "</history>" +
                         "<lifespan>" +
                         "<duration>" +
                         "<sec>300</sec>" +
                         "<nanosec>0</nanosec>" +
                         "</duration>" +
                         "</lifespan>" +
                         "<user_data><value>" + TOPIC_ROUTE_CODE + "</value></user_data>" +
                         "</datawriter_qos>" +
                         "</output>" +
                         "</topic_route>" +
                         "</session>\"");
		
	}

}
