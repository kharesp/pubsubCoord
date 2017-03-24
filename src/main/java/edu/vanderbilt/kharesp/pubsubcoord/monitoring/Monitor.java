package edu.vanderbilt.kharesp.pubsubcoord.monitoring;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.log4j.Logger;


import com.rti.dds.subscription.SampleInfo;
import com.rti.dds.subscription.Subscriber;
import com.rti.dds.topic.Topic;
import com.rti.idl.RTI.RoutingService.Monitoring.DomainRouteStatusSet;
import com.rti.idl.RTI.RoutingService.Monitoring.DomainRouteStatusSetTypeSupport;
import com.rti.idl.RTI.RoutingService.Monitoring.RoutingServiceStatusSet;
import com.rti.idl.RTI.RoutingService.Monitoring.RoutingServiceStatusSetTypeSupport;
import com.rti.idl.RTI.RoutingService.Monitoring.SessionStatusSet;
import com.rti.idl.RTI.RoutingService.Monitoring.SessionStatusSetTypeSupport;
import edu.vanderbilt.kharesp.pubsubcoord.clients.DefaultParticipant;
import edu.vanderbilt.kharesp.pubsubcoord.clients.GenericDataReader;

public class Monitor {
	private static final int MONITORING_DOMAIN_ID= 56;
	private static final String ROUTING_SERVICE_TOPIC = "rti/routing_service/monitoring/routing_service_status_set";
	private static final String DOMAIN_ROUTE_TOPIC = "rti/routing_service/monitoring/domain_route_status_set";
	private static final String SESSION_TOPIC="rti/routing_service/monitoring/session_status_set";


	private Logger logger;
    private AtomicBoolean monitoring;	
    private PrintWriter routingService_status_writer;
    private PrintWriter domainRoute_status_writer;
    private PrintWriter session_status_writer;

	//DDS entities
	private DefaultParticipant participant;
	private Subscriber subscriber;

	//DataReaders 
	private GenericDataReader<RoutingServiceStatusSet> routingService_statusSet_reader;
	private GenericDataReader<DomainRouteStatusSet> domainRoute_statusSet_reader;
	private GenericDataReader<SessionStatusSet> session_statusSet_reader;
	
	private String runId;
	private String znode_path;
	private CuratorFramework client;
	private DistributedBarrier test_started_barrier;
	private DistributedBarrier test_finished_barrier;

	
	public Monitor(String outdir,String runId, String broker_type,String zkConnector) throws Exception{
		this.runId=runId;
		logger= Logger.getLogger(this.getClass().getSimpleName());
		monitoring= new AtomicBoolean(true);
		String processId= ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		String hostName="";
		try {
			hostName=InetAddress.getLocalHost().getHostName();
		} catch (java.net.UnknownHostException e) {
			System.out.println("Host address is not known");
		}
		logger.debug(String.format("Started monitoring Routing Service running on host:%s\n",hostName));
        znode_path=String.format("/experiment/%s/monitoring/%s/monitor_%s", runId,broker_type,hostName);
		try {
			new File(outdir+"/"+runId).mkdirs();
			String rs_status_file= outdir+"/"+runId+"/"+
					"routing_service_"+broker_type+"_"+
					hostName+"_"+processId+".csv";
			String domain_status_file= outdir+"/"+runId+"/"+
					"domain_route_"+broker_type+"_"+
					hostName+"_"+processId+".csv";
			String session_status_file= outdir+"/"+runId+"/"+
					"session_"+broker_type+"_"+
					hostName+"_"+processId+".csv";
			routingService_status_writer= new PrintWriter(rs_status_file,"UTF-8");
			routingService_status_writer.write("ts,date,time,routing service,rs cpu(%),host cpu(%),rs phy mem(kb),host free mem(kb),host total mem(kb)\n");
			domainRoute_status_writer= new PrintWriter(domain_status_file,"UTF-8");
			domainRoute_status_writer.write("ts,date,time,routing service,domain route,input samples/sec,output samples/sec,latency(sec)\n");
			session_status_writer= new PrintWriter(session_status_file,"UTF-8");
			session_status_writer.write("ts,date,time,routing service,domain route,session route,input samples/sec,output samples/sec,latency(sec)\n");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		client=CuratorFrameworkFactory.newClient(zkConnector, new ExponentialBackoffRetry(1000, 3));
		client.start();
		initialize(broker_type);
	}
	
	private void initialize(String broker_type) throws Exception{
		client.create().forPath(znode_path, new byte[0]);
		test_started_barrier = new DistributedBarrier(client, String.format("/experiment/%s/barriers/pub", runId));
		test_finished_barrier = new DistributedBarrier(client, String.format("/experiment/%s/barriers/finished", runId));

		Thread barrierMonitor= new Thread(new Runnable(){
			@Override
			public void run() {
				try {
					test_finished_barrier.waitOnBarrier();
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					monitoring.set(false);
				}
			}
		});
		barrierMonitor.start();
		
		logger.debug("Initializing DDS entities for monitoring");
		
		//Get default participant qos
		//DomainParticipantQos participant_qos= new DomainParticipantQos();
		//DomainParticipantFactory.TheParticipantFactory.get_default_participant_qos(participant_qos);
		//set SHMEM based transport
	    	//participant_qos.transport_builtin.mask = TransportBuiltinKind.SHMEM;

		
	    	participant= new DefaultParticipant(MONITORING_DOMAIN_ID);
		//Register Types
		participant.registerType(RoutingServiceStatusSetTypeSupport.get_instance());
		participant.registerType(DomainRouteStatusSetTypeSupport.get_instance());
		participant.registerType(SessionStatusSetTypeSupport.get_instance());
		
		//Create Topics
		Topic routingServiceStatusSet_topic= participant.create_topic(ROUTING_SERVICE_TOPIC,
				RoutingServiceStatusSetTypeSupport.get_instance());
		Topic domainRouteStatusSet_topic= participant.create_topic(DOMAIN_ROUTE_TOPIC,
				DomainRouteStatusSetTypeSupport.get_instance());
		Topic sessionStatusSet_topic= participant.create_topic(SESSION_TOPIC,
				SessionStatusSetTypeSupport.get_instance());

		//create default subscriber
		subscriber= participant.get_default_subscriber();
		
		// create datareaders for RoutingService, DomainRoute and Session status
		routingService_statusSet_reader = new GenericDataReader<RoutingServiceStatusSet>(subscriber,
				routingServiceStatusSet_topic,RoutingServiceStatusSetTypeSupport.get_instance()){
					private SimpleDateFormat sdf= new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
					@Override
					public void process(RoutingServiceStatusSet sample,SampleInfo info) {
						long millis=System.currentTimeMillis();
						String status= String.format("%d,%s,%s,%f,%f,%f,%f,%d\n",
								millis,
								sdf.format(new Date(millis)),
								sample.name,
								sample.cpu_usage_percentage.publication_period_metrics.mean,
								sample.host_cpu_usage_percentage.publication_period_metrics.mean,
								sample.physical_memory_kb.publication_period_metrics.mean,
								sample.host_free_memory_kb.publication_period_metrics.mean,
								sample.host_total_memory_kb);
						routingService_status_writer.write(status);
					}
		};

		domainRoute_statusSet_reader = new GenericDataReader<DomainRouteStatusSet>(subscriber,
				domainRouteStatusSet_topic,DomainRouteStatusSetTypeSupport.get_instance()){
					private SimpleDateFormat sdf= new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
					@Override
					public void process(DomainRouteStatusSet sample,SampleInfo info) {
						long millis=System.currentTimeMillis();
						String status=String.format("%d,%s,%s,%s,%f,%f,%f\n",
								millis,
								sdf.format(new Date(millis)),
								sample.routing_service_name,sample.name,
								sample.input_samples_per_s.publication_period_metrics.mean,
								sample.output_samples_per_s.publication_period_metrics.mean,
								sample.latency_s.publication_period_metrics.mean);
						domainRoute_status_writer.write(status);
					}
		};
		session_statusSet_reader = new GenericDataReader<SessionStatusSet>(subscriber,
				sessionStatusSet_topic,SessionStatusSetTypeSupport.get_instance()){
					private SimpleDateFormat sdf= new SimpleDateFormat("MM/dd/yyyy,HH:mm:ss");
					@Override
					public void process(SessionStatusSet sample,SampleInfo info) {
					    long millis=System.currentTimeMillis();
						String status=String.format("%d,%s,%s,%s,%s,%f,%f,%f\n",
								millis,
								sdf.format(new Date(millis)),
								sample.routing_service_name,
								sample.domain_route_name,
								sample.name,
								sample.input_samples_per_s.publication_period_metrics.mean,
								sample.output_samples_per_s.publication_period_metrics.mean,
								sample.latency_s.publication_period_metrics.mean
								);
						session_status_writer.write(status);
					}
		};
	}


	public void start_monitoring(){
		try{
			test_started_barrier.waitOnBarrier();;
			// install listeners
			routingService_statusSet_reader.receive();
			domainRoute_statusSet_reader.receive();
			session_statusSet_reader.receive();
			while (monitoring.get()) {
				Thread.sleep(1000);
			}
			client.delete().forPath(znode_path);
		}catch(Exception e){
			logger.error("Monitoring Interrupted");
			e.printStackTrace();
		}finally{
			close_writers();
			CloseableUtils.closeQuietly(client);
			participant.shutdown();
		}
	}
	
	private void close_writers()
	{
		routingService_status_writer.close();
		domainRoute_status_writer.close();
		session_status_writer.close();
	}
	
	
	public static void main(String args[]){
		if(args.length < 4) {
        	System.out.println("Usage: Monitor broker_type outdir runId zkConnector");
        	return;
        }
		String broker_type=args[0];
		String outdir=args[1];
		String runId=args[2];
		String zkConnector=args[3];
		try {
			new Monitor(outdir,runId, broker_type,zkConnector).start_monitoring();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
