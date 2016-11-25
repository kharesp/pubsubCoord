package edu.vanderbilt.kharesp.pubsubcoord.brokers;

import java.lang.management.ManagementFactory;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.rti.connext.infrastructure.WriteSample;
import com.rti.connext.requestreply.Requester;
import com.rti.connext.requestreply.RequesterParams;
import com.rti.dds.domain.DomainParticipant;
import com.rti.dds.domain.DomainParticipantFactory;
import com.rti.dds.infrastructure.InstanceHandleSeq;
import com.rti.dds.infrastructure.StatusKind;
import com.rti.idl.RTI.RoutingService.Administration.CommandKind;
import com.rti.idl.RTI.RoutingService.Administration.CommandRequest;
import com.rti.idl.RTI.RoutingService.Administration.CommandRequestTypeSupport;
import com.rti.idl.RTI.RoutingService.Administration.CommandResponse;
import com.rti.idl.RTI.RoutingService.Administration.CommandResponseTypeSupport;

public class RoutingServiceAdministrator {
	private Logger logger;
	private static final String REQUEST_TOPIC = "rti/routing_service/administration/command_request";
	private static final String RESPONSE_TOPIC = "rti/routing_service/administration/command_response";
	private static final int RS_ADMIN_DOMAIN_ID = 55;
	private static final String TARGET_ROUTER = "PubSubCoord";

	private String hostAddress;
	private String processId;
	private int invocation = 0;
	private DomainParticipant participant;
	private Requester<CommandRequest, CommandResponse> requester = null;

	public RoutingServiceAdministrator(String hostAddress) throws Exception {
		//Configure logger
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.hostAddress=hostAddress;
		processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

		// Create DomainParticipant
		participant = DomainParticipantFactory.get_instance().create_participant(RS_ADMIN_DOMAIN_ID,
				DomainParticipantFactory.PARTICIPANT_QOS_DEFAULT, null, StatusKind.STATUS_MASK_NONE);

		if (participant == null) {
			logger.error("DomainParticipant Creation failure");
			throw new Exception("DomainParticipant Creation failure");
		}
		// Create requester to send command requests  to Routing Service
		try {
			requester = new Requester<CommandRequest, CommandResponse>(new RequesterParams(participant,
					CommandRequestTypeSupport.get_instance(), CommandResponseTypeSupport.get_instance())
							.setRequestTopicName(REQUEST_TOPIC).setReplyTopicName(RESPONSE_TOPIC));

			InstanceHandleSeq handles = new InstanceHandleSeq();
			while (handles.isEmpty()) {
				requester.getRequestDataWriter().get_matched_subscriptions(handles);
				logger.debug(String.format("Routing Service Administrator %s@%s: waiting to discover Routing Service \n",processId,hostAddress));
				Thread.sleep(1000);
			}
			logger.debug(String.format("Routing Service Administrator %s@%s: Routing Service discovered \n",processId,hostAddress));

		} catch (Exception e) {
			logger.error("Failed to create Requester");
			participant.delete_contained_entities();
			DomainParticipantFactory.get_instance().delete_participant(participant);
			throw e;
		}
	}

	public void addPeer(String domainRouteName,String peerLocator,boolean isFirstParticipant) {
		try {
			logger.debug(String.format("Adding Peer:%s for domainRouteName:%s at firstParticipant:%s\n",
					peerLocator,domainRouteName,isFirstParticipant));

            WriteSample<CommandRequest> request = requester.createRequestSample();
            request.getData().id.host = hostAddress.hashCode();
            request.getData().id.app = Integer.parseInt(processId);
            request.getData().id.invocation = ++invocation;
            request.getData().target_router = TARGET_ROUTER;
            request.getData().command._d =
                    CommandKind.RTI_ROUTING_SERVICE_COMMAND_ADD_PEER;
            request.getData().command.peer_desc.domain_route_name = domainRouteName;
            request.getData().command.peer_desc.is_first_participant = isFirstParticipant;
            request.getData().command.peer_desc.peer_list.add(peerLocator);

            requester.sendRequest(request);
            System.out.println("Sent request: "
                    + request.getData().id.host + "(host_id), "
                    + request.getData().id.app + "(app_id), "
                    + request.getData().id.invocation + "(invocation_id)");

        } catch (Exception e) {
			logger.error(e.getMessage(),e);
            throw e;
        }

	}

	public void sendRequest(CommandKind command, String commandString) {
		logger.debug(String.format("Sending command request:%s\n", command.name()));
		try {
			WriteSample<CommandRequest> request = requester.createRequestSample();
			request.getData().id.host = hostAddress.hashCode();
			request.getData().id.app = Integer.parseInt(processId);
			request.getData().id.invocation = ++invocation;
			request.getData().target_router = TARGET_ROUTER;
			request.getData().command._d = command;
			request.getData().command.entity_desc.xml_url.content = commandString;
			request.getData().command.entity_desc.xml_url.is_final = true;
			requester.sendRequest(request);
			logger.debug(String.format("Sent command request:%s with host_id:%s, app_id:%d, invocation id:%d",
					command.name(),request.getData().id.host,request.getData().id.app,request.getData().id.invocation));

		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			throw e;
		}
	}

}
