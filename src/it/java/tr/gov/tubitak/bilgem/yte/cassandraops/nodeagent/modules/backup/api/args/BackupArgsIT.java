package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args;

import org.junit.After;
import org.junit.Test;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.NotADirectoryException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.PathDoesNotExistException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.BackupTestData;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.FileCreationType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants;

import java.io.IOException;

public class BackupArgsIT {

	private BackupTestData testData = null;

	@After
	public void after() throws IOException {
		if (testData != null) {
			testData.deleteRoot();
		}
	}

	@Test
	public void validate_validArgs_success() throws Exception {
		testData = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.CREATE_AS_DIRECTORY)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgs backupArgs = testData.createRealBackupArgs();
		backupArgs.validate();
	}

	@Test(expected = PathDoesNotExistException.class)
	public void validate_cassandraPathDoesNotExist_throwsEx() throws Exception {
		testData = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.CREATE_AS_DIRECTORY)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgs backupArgs = testData.createRealBackupArgs();
		backupArgs.validate();
	}

	@Test(expected = NotADirectoryException.class)
	public void validate_cassandraPathNotADir_throwsEx() throws Exception {
		testData = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.CREATE_AS_FILE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.CREATE_AS_DIRECTORY)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgs backupArgs = testData.createRealBackupArgs();
		backupArgs.validate();
	}

	@Test(expected = PathDoesNotExistException.class)
	public void validate_keyspacePathDoesNotExist_throwsEx() throws Exception {
		testData = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgs backupArgs = testData.createRealBackupArgs();
		backupArgs.validate();
	}

	@Test(expected = NotADirectoryException.class)
	public void validate_keyspacePathNotADir_throwsEx() throws Exception {
		testData = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.CREATE_AS_DIRECTORY)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.CREATE_AS_FILE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgs backupArgs = testData.createRealBackupArgs();
		backupArgs.validate();
	}

}