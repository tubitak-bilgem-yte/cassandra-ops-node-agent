package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.remote;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util.EnvironmentUtil;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.RestoreResultSender;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity.RestoreResponse;

@Component
public class RestoreResultSenderImp implements RestoreResultSender {
	private static final Logger LOGGER = LoggerFactory.getLogger(RestoreResultSenderImp.class);
	private static final String MASTER_ADDRESS_TEMPLATE = "http://%s:%d/batchRestoreResult";
	private static final String ERROR_MESSAGE_TEMPLATE =
			"Results could not sent to master for command restore and relation %s. Status: %s";
	private static final String ERROR_MESSAGE_NODE_TEMPLATE =
			"Unexpected error occured in node %s on command with relation %s.";
	private static final String SUCCESS_MESSAGE_TEMPLATE = "Results sent to master for command restore and relation %s";
	private static final String SENDING_MASTER_INFO = "Sending results to master";
	private EnvironmentUtil environmentUtil;
	private RestTemplate restTemplate;

	@Override
	public String sendRestoreResult(final RestoreResponse result) {

		String message;
		getLogger().info(RestoreResultSenderImp.SENDING_MASTER_INFO);
		try {
			String masterIp = environmentUtil.getMasterPublicIpAddress();
			int masterPort = environmentUtil.getMasterPort();
			String requestAddress = String.format(RestoreResultSenderImp.MASTER_ADDRESS_TEMPLATE, masterIp, masterPort);
			HttpEntity<RestoreResponse> resultReceiverRequest = new HttpEntity<>(result);
			restTemplate.exchange(requestAddress, HttpMethod.POST, resultReceiverRequest, Void.class);
			message = String.format(RestoreResultSenderImp.SUCCESS_MESSAGE_TEMPLATE,
					result.getBatchRelation());
			getLogger().info(message);
		} catch (final HttpClientErrorException | HttpServerErrorException e) {
			message = String.format(RestoreResultSenderImp.ERROR_MESSAGE_TEMPLATE,
					result.getBatchRelation(),
					e.getStatusCode());
			getLogger().error(message);
		} catch (final Exception e) {
			message = String.format(RestoreResultSenderImp.ERROR_MESSAGE_NODE_TEMPLATE,
					result.getCommandResult().getNodeName(),
					result.getBatchRelation());
			getLogger().error(message, e);
		}
		return message;

	}


	@Autowired
	protected void setEnvironmentUtil(final EnvironmentUtil environmentUtil) {
		this.environmentUtil = environmentUtil;
	}

	@Autowired
	protected void setRestTemplate(final RestTemplateBuilder builder) {
		restTemplate = builder.build();
	}

	protected Logger getLogger() {
		return RestoreResultSenderImp.LOGGER;
	}

}
