package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args;

import org.junit.Test;
import org.slf4j.Logger;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.ArgumentValueMandatoryException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.IncorrectPathException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.BackupTestData;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.FileCreationType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants;

import java.nio.file.Paths;

import static org.mockito.Mockito.mock;

public class BackupArgsTest {

	private static final String DATA_PATH = "data";
	private static final String TEMP_BACKUP_PATH = "data";


	@Test
	public void validate_validArgs_success() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_nullCassandraDataPath_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(null, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_emptyCassandraDataPath_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath("", FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_nullTempBackupPath_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(null, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_emptyTempBackupPath_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath("", FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_nullBackupLabel_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(null)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_emptyBackupLabel_throwsEx() throws Exception {
		String backupLabel = "";
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(backupLabel)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_nullBackupType_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(null)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_emptyBackupType_throwsEx() throws Exception {
		String backupType = "";

		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(backupType)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_nullRelation_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(null)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_emptyRelation_throwsEx() throws Exception {
		String relation = "";

		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(relation)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_nullKeyspaces_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.setKeyspaces(null);
		backupArgs.validate();
	}

	@Test(expected = ArgumentValueMandatoryException.class)
	public void validate_emptyKeyspaces_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(TestConstants.TEMP_BACKUP_DIRECTORY_NAME, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	private BackupArgsNoFileSystem createBackupArgs(final BackupTestData data) {
		BackupArgsNoFileSystem backupArgs = new BackupArgsNoFileSystem();
		backupArgs.setBackupType(data.getBackupArgsStub().getBackupType());
		backupArgs.setRelation(data.getBackupArgsStub().getRelation());
		backupArgs.setBackupLabel(data.getBackupArgsStub().getBackupLabel());
		backupArgs.setCassandraDataPath(data.getBackupArgsStub().getCassandraDataPath());
		backupArgs.setKeyspaces(data.getBackupArgsStub().getKeyspaces());
		backupArgs.setTempBackupPath(data.getBackupArgsStub().getTempBackupPath());
		return backupArgs;
	}

	@Test(expected = IncorrectPathException.class)
	public void validate_tempBackupPathIsSameWithCassandraPath_throwsEx() throws Exception {
		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(BackupArgsTest.DATA_PATH, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(BackupArgsTest.TEMP_BACKUP_PATH, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	@Test(expected = IncorrectPathException.class)
	public void validate_tempBackupPathIsChildOfCassandraPath_throwsEx() throws Exception {
		String tempBackupPath = Paths.get(BackupArgsTest.TEMP_BACKUP_PATH, "temp-snapshots").toString();

		BackupTestData data = new BackupTestData.BackupTestDataBuilder()
				.addCassandraDataPath(BackupArgsTest.DATA_PATH, FileCreationType.DO_NOT_CREATE)
				.addTempBackupPath(tempBackupPath, FileCreationType.DO_NOT_CREATE)
				.addBackupLabel(TestConstants.BACKUP_LABEL)
				.addBackupType(BackupType.INCREMENTAL_BACKUP)
				.addKeyspace(TestConstants.KEYSPACE0, FileCreationType.DO_NOT_CREATE)
				.addRelation(TestConstants.RELATION)
				.build();
		BackupArgsNoFileSystem backupArgs = createBackupArgs(data);
		backupArgs.validate();
	}

	private static class BackupArgsNoFileSystem extends BackupArgs {

		BackupArgsNoFileSystem() {
			super();
		}

		@Override
		protected Logger getLogger() {
			return mock(Logger.class);
		}

		@Override
		protected void checkPathExists(final String pathStr) throws NodeAgentException {
			// empty
		}

		@Override
		protected void checkPathIsDirectory(final String pathStr) throws NodeAgentException {
			// empty
		}

		@Override
		protected void checkBackupZipFilesNotExists() throws NodeAgentException {
			// empty
		}
	}
}
