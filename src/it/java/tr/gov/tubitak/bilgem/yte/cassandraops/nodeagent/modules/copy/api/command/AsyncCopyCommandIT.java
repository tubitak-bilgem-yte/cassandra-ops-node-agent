package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.copy.api.command;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.FileUtils;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.copy.api.args.CopyArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResultReceiver;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util.EnvironmentUtil;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AsyncCopyCommandIT {

	private static final String BACKUP_FOLDER_NAME = "backup-1";
	private final String homePath = System.getProperty("user.home");
	private final String backupFolder = Paths.get("copy-backup-test").toString();
	private final String uniqueFolder = Paths.get(homePath, backupFolder, randomUUID().toString()).toString();
	private final String sourceFolder = Paths.get(uniqueFolder, "temp-backups").toString();
	private final String destinationFolder = Paths.get(uniqueFolder, "perma-backups").toString();
	private final String sourceZipFile = Paths.get(sourceFolder, AsyncCopyCommandIT.BACKUP_FOLDER_NAME).toString();
	private final String destinationZipFile = Paths.get(destinationFolder, AsyncCopyCommandIT.BACKUP_FOLDER_NAME).toString();
	private CopyArgs copyArgs;

	@After
	public void after() throws IOException {
		Path path = Paths.get(uniqueFolder);
		if (!path.toFile().exists()) {
			return;
		}
		FileUtils.delete(path);
	}


	@Test
	public void copy_copyBackupZipFilesFromSourceToDestinationFolder_success() throws Exception {
		createTestFoldersAndZipFiles();
		createCopyArgs();
		TestableCopyCommand copyCommand = new TestableCopyCommand();
		CommandResultReceiver commandResult = mock(CommandResultReceiver.class);
		copyCommand.copy(copyArgs, commandResult);
		File destinationFile = new File(destinationZipFile);
		File sourceFile = new File(sourceZipFile);
		Assert.assertTrue(destinationFile.exists());
		assertThat(destinationFile.length(), equalTo(sourceFile.length()));

	}


	private void createTestFoldersAndZipFiles() throws IOException {
		Files.createDirectories(Paths.get(sourceFolder));
		Files.createDirectory(Paths.get(sourceFolder, AsyncCopyCommandIT.BACKUP_FOLDER_NAME));
	}

	private void createCopyArgs() {
		copyArgs = new CopyArgs();
		copyArgs.setCopyRelation(TestConstants.RELATION);
		copyArgs.setSourcePath(sourceZipFile);
		copyArgs.setDestinationPath(destinationZipFile);
		copyArgs.setDeleteSourceAfterCopy(false);
	}


	private class TestableCopyCommand extends AsyncCopyCommand {

		private final EnvironmentUtil environmentUtil;

		TestableCopyCommand() {
			String clusterName = "";
			String nodeName = "";
			this.environmentUtil = mock(EnvironmentUtil.class);
			when(environmentUtil.getClusterName()).thenReturn(clusterName);
			when(environmentUtil.getNodeName()).thenReturn(nodeName);
			setEnvironmentUtil(environmentUtil);
		}

		@Override
		protected void setEnvironmentUtil(final EnvironmentUtil environmentUtil) {
			super.setEnvironmentUtil(environmentUtil);
		}
	}

}