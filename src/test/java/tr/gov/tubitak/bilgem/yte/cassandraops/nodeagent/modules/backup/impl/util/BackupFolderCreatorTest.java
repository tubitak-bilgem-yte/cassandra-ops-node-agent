package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args.BackupArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.NotEnoughDiskSpaceException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

public class BackupFolderCreatorTest {

	private static final long TIMESTAMP = 46546546;
	@Rule
	public MockitoRule rule = MockitoJUnit.rule();

	private BackupArgs backupArgs;
	@Spy
	private BackupFolderCreator backupFolderCreator;

	@Before
	public void init() {
		backupArgs = new BackupArgs();
		doNothing().when(backupFolderCreator).readKeyspacesFromDisk(any());
	}


	@Test
	public void checkAvailableSpace_haveEnoughSpace_success() throws Exception {
		checkAvaliableSpaceHelper(500L, 50L, BackupType.INCREMENTAL_BACKUP);
		backupFolderCreator.checkAvailableSpace(BackupFolderCreatorTest.TIMESTAMP);


	}

	@Test
	public void checkAvailableSpace_haveJustEnoughSpace_success() throws Exception {
		checkAvaliableSpaceHelper(500L, 500L, BackupType.INCREMENTAL_BACKUP);
		backupFolderCreator.checkAvailableSpace(BackupFolderCreatorTest.TIMESTAMP);


	}

	@Test(expected = NotEnoughDiskSpaceException.class)
	public void checkAvailableSpace_notEnoughSpace_throwEx() throws Exception {
		checkAvaliableSpaceHelper(50L, 500L, BackupType.INCREMENTAL_BACKUP);
		backupFolderCreator.checkAvailableSpace(BackupFolderCreatorTest.TIMESTAMP);

	}

//    @Test
//    public void createTempBackupFolder_success() throws Exception {
//        checkAvaliableSpaceHelper(500L, 50L,BackupType.INCREMENTAL_BACKUP);
//        backupFolderCreator.createTempBackupFolder(BackupFolderCreatorTest.TIMESTAMP);
//
//    }

	private void checkAvaliableSpaceHelper(final long avaliableSpace, final long backupSize, final String backupType) throws Exception {
		setBackupArgsParameter(backupType);
		doReturn(avaliableSpace).when(backupFolderCreator).getUsableSpace(any());
		doReturn(backupSize).when(backupFolderCreator).getTotalIncrementalBackupSize();
		doReturn(backupSize).when(backupFolderCreator).getTotalSnapshotSize();
	}

	private void setBackupArgsParameter(final String backupType) {
		backupArgs.setBackupType(backupType);
		backupArgs.setTempBackupPath("");
		backupArgs.setBackupLabel("label");
		backupFolderCreator.setBackupArgs(backupArgs);
	}


}