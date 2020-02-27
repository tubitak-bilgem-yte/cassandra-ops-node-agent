package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args.BackupArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.BackupTestData;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.FileCreationType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants.*;

public class BackupCleanerIT {
	private BackupTestData backupData;
	private TestableBackupCleaner backupCleaner;

	@Before
	public void init() throws IOException {
		createIncrementalBackupData();
		backupCleaner = new TestableBackupCleaner(backupData.createRealBackupArgs());
	}

	@After
	public void after() throws IOException {
		backupData.deleteRoot();
	}

	@Test
	public void deleteIncrementalBackupFiles_deletesOnlyAndOnlyBackupFiles_successful() throws IOException {
		BackupArgs backupArgs = backupData.getBackupArgsStub();
		File mainCassandraDataDirectory = new File(backupArgs.getCassandraDataPath());
		List<String> expectedDirectoryNames = getAllFilesAndDirectoriesForGivenFile(mainCassandraDataDirectory, true);
		backupCleaner.deleteIncrementalBackupFiles(System.currentTimeMillis());
		List<String> actualDirectoryNames = getAllFilesAndDirectoriesForGivenFile(mainCassandraDataDirectory, false);
		assertThat(expectedDirectoryNames, containsInAnyOrder(actualDirectoryNames.toArray()));
	}

	private List<String> getAllFilesAndDirectoriesForGivenFile(final File rootFile, final boolean isBeforeDeletionTraverse) throws IOException {
		Deque<File> fileTraverseStack = new ArrayDeque<>();
		List<String> directoryNames = new ArrayList<>();
		fileTraverseStack.push(rootFile);
		while (!fileTraverseStack.isEmpty()) {
			File currentFile = fileTraverseStack.pop();
			directoryNames.add(currentFile.getCanonicalPath());
			if (currentFile.isDirectory()) {
				//To compare contents of the two trees, before-cleaning traversal should not include the contents of the backup folder, but afte-cleaning traverse should include them
				//since
				if (currentFile.getName().equals("backups") && isBeforeDeletionTraverse) {
					continue;
				}
				fileTraverseStack.addAll(Arrays.asList(currentFile.listFiles()));
			}

		}
		return directoryNames;

	}

	private void createIncrementalBackupData() throws IOException {
		String homePath = Paths.get(HOME_DIRECTORY, BACKUP_DIRECTORY).toString();
		backupData = new BackupTestData.BackupTestDataBuilder()
				.setHomePath(homePath)
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TEST_KEYSPACE, FileCreationType.CREATE_AS_DIRECTORY)
				.addTable(TEST_TABLE_1)
				.addIncrementalBackupFile(TEST_TABLE_1_FILE_1)
				.addIncrementalBackupFile(TEST_TABLE_1_FILE_2)
				.addTable(TEST_TABLE_2)
				.addIncrementalBackupFile(TEST_TABLE_2_FILE_1)
				.addIncrementalBackupFile(TEST_TABLE_2_FILE_2)
				.addIncrementalBackupFile(TEST_TABLE_2_FILE_3)
				.addIncrementalBackupFile(TEST_TABLE_2_FILE_4)
				.addTable(TEST_TABLE_3)
				.addIncrementalBackupFile(TEST_TABLE_3_FILE_1)
				.addIncrementalBackupFile(TEST_TABLE_3_FILE_2)
				.addIncrementalBackupFile(TEST_TABLE_3_FILE_3)
				.addTable(TEST_TABLE_4)
				.addIncrementalBackupFile(TEST_TABLE_4_FILE_1)
				.build();
	}

	private class TestableBackupCleaner extends BackupCleaner {

		public TestableBackupCleaner(final BackupArgs backupArgs) {
			super(backupArgs);
		}
	}

}