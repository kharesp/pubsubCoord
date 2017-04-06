package edu.vanderbilt.kharesp.pubsubcoord.monitoring;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Monitor {
	private CuratorFramework client;
	private String znodePath;
	private DistributedBarrier testStartedBarrier;
	private DistributedBarrier testFinishedBarrier;
	
	private long startTs;
	private long endTs;

	private String nwStatsFile;
	private String utilStatsFile;

	private Logger logger;

	public Monitor(String zkConnector, String brokerType,
			String runId, String outDir){
		
		logger=  Logger.getLogger(this.getClass().getSimpleName());
		
		String processId= ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		String hostName="";
		try {
			hostName=InetAddress.getLocalHost().getHostName();
		} catch (java.net.UnknownHostException e) {
			logger.error(e.getMessage(),e);
		}	

		new File(outDir+"/"+runId).mkdirs();
		utilStatsFile=String.format("%s/%s/util_%s_%s_%s.csv",outDir,runId,brokerType,hostName,processId);
		nwStatsFile=String.format("%s/%s/nw_%s_%s_%s.csv",outDir,runId,brokerType,hostName,processId);
		

		client= CuratorFrameworkFactory.newClient(zkConnector, new ExponentialBackoffRetry(1000, 3));
		client.start();
		znodePath=String.format("/experiment/%s/monitoring/%s/monitor_%s",runId,brokerType,hostName);
		testStartedBarrier=new DistributedBarrier(client, String.format("/experiment/%s/barriers/pub", runId));
		testFinishedBarrier=new DistributedBarrier(client, String.format("/experiment/%s/barriers/finished", runId));
	}
	
	public void start_monitoring(){
		try {
			//Create znode for this monitoring process
			client.create().forPath(znodePath, new byte[0]);
			
			//wait until test starts
			testStartedBarrier.waitOnBarrier();
		
			//record start ts
			startTs=Instant.now().toEpochMilli();
			
			//wait until test ends
			testFinishedBarrier.waitOnBarrier();
		
			//record end ts
			endTs=Instant.now().toEpochMilli();

			//collect sysstat metrics 
			collectStats(startTs,endTs);
			
			//delete znode for this monitoring process before exiting
			client.delete().forPath(znodePath);
			
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}finally{
			CloseableUtils.closeQuietly(client);
		}
	}
	
	private void collectStats(long startTs,long endTs){
		SimpleDateFormat formatter= new SimpleDateFormat("HH:mm:ss");
		String startTime= formatter.format(new Date(startTs));
		String endTime= formatter.format(new Date(endTs));

		//collect system utilization metrics 
		String command=String.format("sadf -s %s -e %s -U -h -d -- -ur",startTime,endTime);
		executeSystemCommand(command,utilStatsFile);

		//collect network utilization metrics
		command=String.format("sadf -s %s -e %s -U -h -d -- -n DEV",startTime,endTime);
		executeSystemCommand(command,nwStatsFile);
	}
	
	private void executeSystemCommand(String command,String output){
		PrintWriter writer=null;
		try {
			writer= new PrintWriter(output,"UTF-8");
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stdInp = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader stdErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line;
			while ((line=stdInp.readLine()) != null) {
                writer.write(line);
            }
			while ((line=stdErr.readLine()) != null) {
				logger.error(line);
            }
			stdInp.close();
			stdErr.close();
            
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}finally{
			writer.close();
		}
	}
	
	public static void Main(String args[]){

		if(args.length < 4){
			System.out.println("Usage: Monitor brokerType outDir runId zkConnector");
			return;
		}
		PropertyConfigurator.configure("log4j.properties");
		String brokerType=args[0];
		String outDir=args[1];
		String runId=args[2];
		String zkConnector=args[3];
		new Monitor(zkConnector,brokerType,runId,outDir).start_monitoring();
	}

}
