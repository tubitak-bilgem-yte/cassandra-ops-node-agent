package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.BackupTestData;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.FileCreationType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants.*;

public class BackupFolderCreatorIT {

	private static final String BACKUP_WITH_SIZE = "backup_with_size";
	private BackupTestData incrementalBackupTestData;
	private BackupTestData snapshotBackupData;
	@Rule
	public MockitoRule rule = MockitoJUnit.rule();

	private BackupTestData.BackupTestDataBuilder incrementalBackupTestDataBuilder;
	private BackupTestData.BackupTestDataBuilder snapshotBackupTestDataBuilder;

	@Spy
	private BackupFolderCreator backupFolderCreator;
	private long expectedBackupFileSize;

	private final String[] directoryNameArray = {
			"urunhareketleri",
			"bildirim",
			"askidaki_bildirim",
			"titck_tekil_urun_hareketleri",
			"urun"
	};

	private final String[] fileNameArray = {
			"bildirim_ibackup1",
			"bildirim_ibackup2",

			"askidaki_bildirim_ibackup1",
			"askidaki_bildirim_ibackup2",
			"askidaki_bildirim_ibackup3",
			"askidaki_bildirim_ibackup4",

			"titck_tekil_urun_hareketleri_ibackup1",
			"titck_tekil_urun_hareketleri_ibackup2",
			"titck_tekil_urun_hareketleri_ibackup3",

			"urun_ibackup1"
	};

	@After
	public void after() throws IOException {
		if (incrementalBackupTestData != null) {
			incrementalBackupTestData.deleteRoot();
		}

		if (snapshotBackupData != null) {
			snapshotBackupData.deleteRoot();
		}
	}

	@Test
	public void getTotalIncrementalBackupSize_checkBackupSizeMatchesInsertedFilesSize_success() throws IOException {
		expectedBackupFileSize = 1024 * 1024 * 20;
		createIncrementalBackupDataBuilder();
		incrementalBackupTestDataBuilder.addIncrementalBackupFile(BackupFolderCreatorIT.BACKUP_WITH_SIZE, expectedBackupFileSize);
		incrementalBackupTestData = incrementalBackupTestDataBuilder.build();
		backupFolderCreator = new BackupFolderCreator(incrementalBackupTestData.createRealBackupArgs());
		final long actualBackupFileSize = backupFolderCreator.getTotalIncrementalBackupSize();
		assertThat(expectedBackupFileSize, equalTo(actualBackupFileSize));

	}

	@Test
	public void getTotalSnapshotBackupSize_checkBackupSizeMatchesInsertedFilesSize_success() throws IOException {
		expectedBackupFileSize = 1024 * 1024 * 20;
		createSnapshotBackupDataBuilder();
		snapshotBackupTestDataBuilder.addSnapshotFile(BackupFolderCreatorIT.BACKUP_WITH_SIZE, expectedBackupFileSize);
		snapshotBackupData = snapshotBackupTestDataBuilder.build();
		backupFolderCreator = new BackupFolderCreator(snapshotBackupData.createRealBackupArgs());
		final long actualBackupFileSize = backupFolderCreator.getTotalSnapshotSize();
		assertThat(expectedBackupFileSize, equalTo(actualBackupFileSize));
	}

	@Test
	public void createTempBackupFolder_success() throws IOException, NodeAgentException {
		expectedBackupFileSize = 1024 * 1024 * 20;
		createSnapshotBackupDataBuilder();
		snapshotBackupTestDataBuilder.addSnapshotFile(BackupFolderCreatorIT.BACKUP_WITH_SIZE, expectedBackupFileSize);
		snapshotBackupData = snapshotBackupTestDataBuilder.build();
		backupFolderCreator = new BackupFolderCreator(snapshotBackupData.createRealBackupArgs());
		backupFolderCreator.createTempBackupFolder(1L);
		Boolean expectedResult = true;
		Boolean actualResult = checkBackupFileDirectory(snapshotBackupData);
		assertThat(actualResult, equalTo(expectedResult));
	}


	private void createIncrementalBackupDataBuilder() throws IOException {
		String homePath = Paths.get(HOME_DIRECTORY, BACKUP_DIRECTORY).toString();
		incrementalBackupTestDataBuilder = new BackupTestData.BackupTestDataBuilder()
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
				.addIncrementalBackupFile(TEST_TABLE_4_FILE_1);
	}

	private void createSnapshotBackupDataBuilder() throws IOException {
		String homePath = Paths.get(HOME_DIRECTORY, BACKUP_DIRECTORY).toString();
		snapshotBackupTestDataBuilder = new BackupTestData.BackupTestDataBuilder()
				.setHomePath(homePath)
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.SNAPSHOT)
				.addKeyspace(TEST_KEYSPACE, FileCreationType.CREATE_AS_DIRECTORY)
				.addTable(TEST_TABLE_1)
				.addSnapshotFile(TEST_TABLE_1_FILE_1)
				.addSnapshotFile(TEST_TABLE_1_FILE_2)
				.addTable(TEST_TABLE_2)
				.addSnapshotFile(TEST_TABLE_2_FILE_1)
				.addSnapshotFile(TEST_TABLE_2_FILE_2)
				.addSnapshotFile(TEST_TABLE_2_FILE_3)
				.addSnapshotFile(TEST_TABLE_2_FILE_4)
				.addTable(TEST_TABLE_3)
				.addSnapshotFile(TEST_TABLE_3_FILE_1)
				.addSnapshotFile(TEST_TABLE_3_FILE_2)
				.addSnapshotFile(TEST_TABLE_3_FILE_3)
				.addTable(TEST_TABLE_4)
				.addSnapshotFile(TEST_TABLE_4_FILE_1);
	}

	private boolean checkBackupFileDirectory(final BackupTestData backupTestData) {
		Path backupPath = Paths.get(backupTestData.getHomePath(), backupTestData.getRootDirName());
		return new File(backupPath.toString()).exists();
	}


}