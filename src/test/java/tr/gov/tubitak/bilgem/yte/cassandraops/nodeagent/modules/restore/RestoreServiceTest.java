package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.slf4j.Logger;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity.RestoreRequest;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class RestoreServiceTest {

	@Rule
	public MockitoRule rule = MockitoJUnit.rule();
	@Spy
	private RestoreService restoreService;
	@Mock
	private Logger mockLogger;
	private RestoreRequest restoreRequest;

	@Before
	public void init() {
		restoreRequest = new RestoreRequest();
		restoreRequest.setBatchBackupFolders(generateFilesList());
	}

	@Test
	public void getLastBackupName_success() {
		String expectedLastUniqueName = "ib1902251053";
		String actualLastUniqueName = restoreService.getLastBackupName(restoreRequest.getBatchBackupFolders());
		assertThat(actualLastUniqueName, equalTo(expectedLastUniqueName));

	}

	private List<String> generateFilesList() {

		return new ArrayList<String>() {
			{
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\sb1902250953_relation_nodename1");
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\sb1902251053_relation_nodename2");
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\ib1902251153_relation_nodename1");
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\ib1902250953_relation_nodename1");
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\ib1902250953_relation_nodename2");
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\ib1902250953_relation_nodename3");
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\ib1902250953_relation_nodename4");
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\ib1902251053_relation_nodename1");
				add("Discname:\\Users\\user.name\\backups\\clustername\\backupname\\ib1902251053_relation_nodename2");
			}
		};

	}

}