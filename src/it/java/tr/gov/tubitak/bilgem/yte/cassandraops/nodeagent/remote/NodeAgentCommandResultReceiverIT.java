package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.remote;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResult;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util.EnvironmentUtil;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;


@RunWith(SpringJUnit4ClassRunner.class)
@RestClientTest(NodeAgentCommandResultReceiver.class)
public class NodeAgentCommandResultReceiverIT {

	private static final String ERROR_MESSAGE_TEMPLATE =
			"Results could not sent to master for command %s and relation %s. Status: %s";
	private static final String SUCCESS_MESSAGE_TEMPLATE = "Results sent to master for command %s and relation %s";
	private static final String REQUEST_ADDRESS = "http://178.0.0.1:28080/receiveResult";
	@Autowired
	private MockRestServiceServer server;
	@Autowired
	private NodeAgentCommandResultReceiver nodeAgentCommandResultReceiver;
	@MockBean
	private EnvironmentUtil environmentUtil;
	private CommandResult commandResult;

	@Before
	public void init() {
		String masterPublicIpAddress = "178.0.0.1";
		int masterPort = 28080;
		String commandName = "copy";
		String relation = "abc123";
		when(environmentUtil.getMasterPublicIpAddress()).thenReturn(masterPublicIpAddress);
		when(environmentUtil.getMasterPort()).thenReturn(masterPort);
		commandResult = new CommandResult();
		commandResult.setCommandName(commandName);
		commandResult.setRelation(relation);
	}

	@Test
	public void receiveResult_sendCopyOrBackupOperationResultToMaster_success() throws Exception {
		server.expect(requestTo(NodeAgentCommandResultReceiverIT.REQUEST_ADDRESS))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withStatus(HttpStatus.OK));
		String expectedResponse = String.format(NodeAgentCommandResultReceiverIT.SUCCESS_MESSAGE_TEMPLATE, commandResult.getCommandName(), commandResult.getRelation());
		String actualResponse = nodeAgentCommandResultReceiver.sendCommandResult(commandResult);
		assertThat(actualResponse, equalTo(expectedResponse));
	}

	@Test
	public void receiveResult_sendCopyOrBackupOperationResultToMaster_throwsHttpClientErrorException() throws Exception {
		String serverResponseString = "404";
		server.expect(requestTo(NodeAgentCommandResultReceiverIT.REQUEST_ADDRESS))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withStatus(HttpStatus.NOT_FOUND));
		String expectedResponse = String.format(NodeAgentCommandResultReceiverIT.ERROR_MESSAGE_TEMPLATE,
				commandResult.getCommandName(),
				commandResult.getRelation(),
				serverResponseString);
		String actualResponse = nodeAgentCommandResultReceiver.sendCommandResult(commandResult);
		assertThat(actualResponse, equalTo(expectedResponse));
	}

	@Test
	public void receiveResult_sendCopyOrBackupOperationResultToMaster_throwsHttpServerErrorException() throws Exception {
		String serverResponseString = "500";
		server.expect(requestTo(NodeAgentCommandResultReceiverIT.REQUEST_ADDRESS))
				.andExpect(method(HttpMethod.POST))
				.andRespond(withStatus(HttpStatus.INTERNAL_SERVER_ERROR));
		String expectedResponse = String.format(NodeAgentCommandResultReceiverIT.ERROR_MESSAGE_TEMPLATE,
				commandResult.getCommandName(),
				commandResult.getRelation(),
				serverResponseString);
		String actualResponse = nodeAgentCommandResultReceiver.sendCommandResult(commandResult);
		assertThat(actualResponse, equalTo(expectedResponse));
	}
}