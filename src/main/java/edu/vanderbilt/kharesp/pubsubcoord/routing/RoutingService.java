package edu.vanderbilt.kharesp.pubsubcoord.routing;

import java.util.HashMap;
import java.util.Map;

public class RoutingService {

	private String routingServiceName;
	private Map<String,DomainRoute> domain_routes;

	public RoutingService(String name){
		this.routingServiceName=name;
		this.domain_routes=new HashMap<String,DomainRoute>();
	}
	
	public void createDomainRoute(String domainRouteName,String type) throws Exception
	{
		DomainRoute route=new DomainRoute(domainRouteName,type);
		domain_routes.put(domainRouteName, route);
	}
	
	public void createTopicSession(String domainRouteName,String topic_name,
			String type_name,String session_type){
		domain_routes.get(domainRouteName).createTopicSession(topic_name, type_name, session_type);
	}

	public String getName()
	{
		return routingServiceName;
	}
	

}
