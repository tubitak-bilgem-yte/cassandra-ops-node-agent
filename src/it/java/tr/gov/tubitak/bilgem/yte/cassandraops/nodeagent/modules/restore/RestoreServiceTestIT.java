package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore;

import org.apache.cassandra.tools.BulkLoadException;
import org.apache.cassandra.tools.BulkLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.CommandRunnerUtil;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util.EnvironmentUtil;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.dao.RestoreTestEntity;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity.RestoreRequest;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants.BACKUP_DIRECTORY;
import static tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants.HOME_DIRECTORY;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestoreServiceTestConfiguration.class)
@TestPropertySource(locations = "/test-application.properties")
public class RestoreServiceTestIT {
	private final String backupTableFolder = Paths.get(HOME_DIRECTORY, BACKUP_DIRECTORY, "cluster1", "test_backup_name", "nodeagent_test", "restore_test_table").toString();
	private final String backupFolder = Paths.get(HOME_DIRECTORY, BACKUP_DIRECTORY, "cluster1", "test_backup_name").toString();

	private RestoreService restoreService;
	private RestoreRequest restoreRequest;
	private final EnvironmentUtil environmentUtil = mock(EnvironmentUtil.class);

	@Value("${nodeagent.cassandra.username}")
	private String cassandraUserName;

	@Value("${nodeagent.cassandra.password}")
	private String cassandraPassword;

	@Value("${nodeagent.cassandra.cassandraBinDir}")
	private String cassandraBinDir;

	@Value("${nodeagent.cassandra.nodePublicIpAddress}")
	private String nodeIpAddress;

	@Autowired
	private CassandraAdminTemplate cassandraAdminTemplate;

	public void init() {
		RestoreResultSender restoreResultSender = mock(RestoreResultSender.class);
		CommandRunnerUtil commandRunnerUtil = new CommandRunnerUtil();
		when(environmentUtil.getCassandraUserName()).thenReturn(cassandraUserName);
		when(environmentUtil.getCassandraPassword()).thenReturn(cassandraPassword);
		when(environmentUtil.getCassandraBinDir()).thenReturn(cassandraBinDir);
		when(environmentUtil.getNodePublicIpAddress()).thenReturn(nodeIpAddress);

		restoreService = new RestoreService();
		restoreRequest = new RestoreRequest();
		restoreRequest.setNodeName("Node_1");
		restoreRequest.setClusterName("Cluster_1");
		restoreRequest.setBatchRelation("Batch_Relation");
		restoreRequest.setParentRelation("Parent_Relation");
		restoreRequest.setClusterName("Cluster_1");
		List<String> backupFoldersList = new ArrayList<>();
		backupFoldersList.add(backupFolder);
		restoreRequest.setBatchBackupFolders(backupFoldersList);
		List<String> keyspacesList = new ArrayList<>();
		keyspacesList.add("nodeagent_test");
		restoreRequest.setRestoreKeyspaces(keyspacesList);
		restoreService.setRestoreResultSender(restoreResultSender);


		commandRunnerUtil.setEnvironment(environmentUtil);

		restoreService.setCommandRunnerUtil(commandRunnerUtil);


	}


	@Test
	public void restore_shouldRestoreToOriginalValues_bulkLoader_success() throws BulkLoadException {
		List<RestoreTestEntity> originalTestValues = cassandraAdminTemplate.select("select* from restore_test_table", RestoreTestEntity.class);

		cassandraAdminTemplate.truncate(RestoreTestEntity.class);
		String[] cmds = {
				"-d localhost",
				backupTableFolder
		};

		BulkLoader.main(cmds);

		List<RestoreTestEntity> restoredValues = cassandraAdminTemplate.select("select * from restore_test_table", RestoreTestEntity.class);
		assertThat(restoredValues, equalTo(originalTestValues));
	}

	@Test
	public void restore_shouldRestoreToOriginalValues_restoreService_success() throws BulkLoadException, InterruptedException {
		init();
		List<RestoreTestEntity> originalTestValues = cassandraAdminTemplate.select("select * from restore_test_table", RestoreTestEntity.class);

		cassandraAdminTemplate.truncate(RestoreTestEntity.class);

		restoreService.restore(restoreRequest);

		List<RestoreTestEntity> restoredValues = cassandraAdminTemplate.select("select * from restore_test_table", RestoreTestEntity.class);
		assertThat(restoredValues, containsInAnyOrder(originalTestValues.toArray()));
	}

}