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
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResult;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResultReceiver;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util.EnvironmentUtil;

@Component
public class NodeAgentCommandResultReceiver implements CommandResultReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeAgentCommandResultReceiver.class);
    private static final String MASTER_ADDRESS_TEMPLATE = "http://%s:%d/receiveResult";
    private static final String ERROR_MESSAGE_TEMPLATE =
            "Results could not sent to master for command %s and relation %s. Status: %s";
    private static final String ERROR_MESSAGE_NODE_TEMPLATE =
            "Unexpected error occured in node %s on command with relation %s.";
    private static final String SUCCESS_MESSAGE_TEMPLATE = "Results sent to master for command %s and relation %s";
    private static final String SENDING_MASTER_INFO = "Sending results to master";

    private EnvironmentUtil environmentUtil;
    private RestTemplate restTemplate;

    /**
     * Commands such as copy and backup are Async. When they complete, they send their results via calling this method.
     * This method sends the results to master.
     */
    @Override
    public String sendCommandResult(final CommandResult result) {
        String message;
        getLogger().info(NodeAgentCommandResultReceiver.SENDING_MASTER_INFO);
        try {
            String masterIp = environmentUtil.getMasterPublicIpAddress();
            int masterPort = environmentUtil.getMasterPort();
            String requestAddress = String.format(NodeAgentCommandResultReceiver.MASTER_ADDRESS_TEMPLATE, masterIp, masterPort);
            HttpEntity<CommandResult> resultReceiverRequest = new HttpEntity<>(result);
            restTemplate.exchange(requestAddress, HttpMethod.POST, resultReceiverRequest, Void.class);
            message = String.format(NodeAgentCommandResultReceiver.SUCCESS_MESSAGE_TEMPLATE,
                    result.getCommandName(),
                    result.getRelation());
            getLogger().info(message);
        } catch (final HttpClientErrorException | HttpServerErrorException e) {
            message = String.format(NodeAgentCommandResultReceiver.ERROR_MESSAGE_TEMPLATE,
                    result.getCommandName(),
                    result.getRelation(),
                    e.getStatusCode());
            getLogger().error(message);
        } catch (final Exception e) {
            message = String.format(NodeAgentCommandResultReceiver.ERROR_MESSAGE_NODE_TEMPLATE,
                    result.getNodeName(),
                    result.getRelation());
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
        return NodeAgentCommandResultReceiver.LOGGER;
    }


}
