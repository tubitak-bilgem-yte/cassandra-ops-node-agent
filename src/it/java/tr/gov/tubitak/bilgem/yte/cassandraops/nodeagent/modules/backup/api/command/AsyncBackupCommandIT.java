package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.command;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AsyncBackupCommandIT {

	private final String homePath = System.getProperty("user.home");
	private final String testBackupPath = Paths.get(homePath, "backups", UUID.randomUUID().toString()).toString();

	private final String[] exampleZipFleNames = {
			"sb654654_jhsd876_node1",
			"ib4684684_54sfa87_node2",
			"ib4684684_54sfa87_node3",
			"sb487497_faw75s7_node4"
	};

	@After
	public void cleanup() throws IOException {
		FileUtils.deleteDirectory(new File(testBackupPath));
	}

	@Test
	public void getBackupRelationsFromBackupPath_getBackupRelations_success() throws IOException {
		createTestZipFiles();
		AsyncBackupCommand asyncBackupCommand = new AsyncBackupCommand();
		Set<String> actualRelationNames = asyncBackupCommand.getBackupRelationsFromBackupPath(testBackupPath);
		Set<String> expectedRelationNames = new HashSet<>(Arrays.asList("jhsd876", "54sfa87", "faw75s7"));
		assertThat(actualRelationNames, equalTo(expectedRelationNames));
	}

	private void createTestZipFiles() throws IOException {
		Files.createDirectories(Paths.get(testBackupPath));
		for (String fileName : exampleZipFleNames) {
			Files.createDirectory(Paths.get(testBackupPath, fileName));
		}
	}
}