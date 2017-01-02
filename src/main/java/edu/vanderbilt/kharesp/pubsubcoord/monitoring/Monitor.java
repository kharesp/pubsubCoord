package edu.vanderbilt.kharesp.pubsubcoord.monitoring;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import com.rti.dds.subscription.Subscriber;
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
	private String hostAddress;
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

	
	public Monitor(String outdir,String runId) throws Exception{
		logger= Logger.getLogger(this.getClass().getSimpleName());
		monitoring= new AtomicBoolean(true);
		try {
			hostAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (java.net.UnknownHostException e) {
			System.out.println("Host address is not known");
		}
		logger.debug(String.format("Started monitoring Routing Service running on host:%s\n",hostAddress));
		try {
			new File(outdir+"/"+runId).mkdirs();
			String rs_status_file= outdir+"/"+runId+"/"+
					"routing_service_"+
					hostAddress+".csv";
			String domain_status_file= outdir+"/"+runId+"/"+
					"domain_route_"+
					hostAddress+".csv";
			String session_status_file= outdir+"/"+runId+"/"+
					"session_"+
					hostAddress+".csv";
			routingService_status_writer= new PrintWriter(rs_status_file,"UTF-8");
			domainRoute_status_writer= new PrintWriter(domain_status_file,"UTF-8");
			session_status_writer= new PrintWriter(session_status_file,"UTF-8");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		initialize();
	}
	
	private void initialize() throws Exception{
		logger.debug("Initializing DDS entities for monitoring");
		//create default participant
		participant= new DefaultParticipant(MONITORING_DOMAIN_ID);

		//create default subscriber
		subscriber= participant.get_default_subscriber();
		
		// create datareaders for RoutingService, DomainRoute and Session status
		routingService_statusSet_reader = new GenericDataReader<RoutingServiceStatusSet>(subscriber,
				ROUTING_SERVICE_TOPIC,RoutingServiceStatusSetTypeSupport.get_instance()){
					@Override
					public void process(RoutingServiceStatusSet sample) {
						System.out.println(sample.toString());
					}
		};

		domainRoute_statusSet_reader = new GenericDataReader<DomainRouteStatusSet>(subscriber,
				DOMAIN_ROUTE_TOPIC,DomainRouteStatusSetTypeSupport.get_instance()){
					@Override
					public void process(DomainRouteStatusSet sample) {
						System.out.println(sample);
					}
		};
		session_statusSet_reader = new GenericDataReader<SessionStatusSet>(subscriber,
				SESSION_TOPIC,SessionStatusSetTypeSupport.get_instance()){
					@Override
					public void process(SessionStatusSet sample) {
						System.out.println(sample);
					}
		};
	}

	private void close_writers()
	{
		routingService_status_writer.close();
		domainRoute_status_writer.close();
		session_status_writer.close();
	}

	public void start_monitoring(){
		logger.debug("Installing listeners for receiving monitoring data");

		//install listeners to receive monitoring data
		//routingService_statusSet_reader.receive();
		//domainRoute_statusSet_reader.receive();
		session_statusSet_reader.receive();

		while(monitoring.get()){
			try{
				Thread.sleep(1000);
			}catch(InterruptedException e){
				logger.error("Monitoring Interrupted");
				close_writers();
				participant.shutdown();
			}
		}
	}
	
	public void stop_monitoring(){
		logger.debug("Stopping monitoring service");
		monitoring.set(false);
		close_writers();
		participant.shutdown();
	}
	
	public static void main(String args[]){
		if(args.length < 2) {
        	System.out.println("Usage: Monitor outdir runId");
        	return;
        }
		String outdir=args[0];
		String runId=args[1];
		try {
			new Monitor(outdir,runId).start_monitoring();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
