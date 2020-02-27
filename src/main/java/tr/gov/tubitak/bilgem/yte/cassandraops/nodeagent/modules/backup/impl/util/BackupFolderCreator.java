package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util;

import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args.BackupArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.NotEnoughDiskSpaceException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema.Keyspace;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema.Table;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@NoArgsConstructor
public class BackupFolderCreator {
	private static final Logger LOGGER = LoggerFactory.getLogger(BackupFolderCreator.class);
	private static final String NO_SNAPSHOTS_FOUND_INFO = "No snapshots found for %s.%s";
	private static final String SNAPSHOT_DIRECTORY_IS_NOT_A_DIRECTORY_WARNING = "There is a file identical to snapshot label %s " +
			"in the %s-%s's snapshot directory and this file is not a directory.";
	private static final String NO_INCREMENTAL_BACKUPS_FOUND_INFO = "No incremental backups found for %s.%s";
	private static final String NOT_ENOUGH_DISK_SPACE_ERROR = "There should be at least %d MB free space " +
			"(which is %d * total backup size (%d MB)) in this " +
			"path's partition (%s). But there was only %d MB.";
	private BackupArgs backupArgs;
	private List<Keyspace> keyspaces;

	public BackupFolderCreator(final BackupArgs backupArgs) {
		this.backupArgs = backupArgs;
		readKeyspacesFromDisk(backupArgs);

	}

	/**
	 * This is the main method to create the backup file that will include sstables of snapshots or
	 * incremental backups. First, it checks the avaliable space for whether the system has enough
	 * space for the backup file. If it has space it creates the file in temp backup folder and puts
	 * the sstables in proper directory structure into the backup folder one by one.
	 */
	public void createTempBackupFolder(final long backupTimestamp) throws IOException, NodeAgentException {
		checkAvailableSpace(backupTimestamp);
		if (backupArgs.getBackupType().equals(BackupType.INCREMENTAL_BACKUP)) {
			createIncrementalBackupFolder(backupTimestamp);
		} else {
			createSnapshotFolder(backupTimestamp);
		}
	}

	private void createIncrementalBackupFolder(final long backupTimestamp) throws IOException {
		createBackupFolder(BackupType.INCREMENTAL_BACKUP, backupTimestamp);
	}


	protected void createSnapshotFolder(final long backupTimestamp) throws IOException {
		createBackupFolder(BackupType.SNAPSHOT, backupTimestamp);
	}

	protected void createBackupFolder(final String backupType, final long backupTimestamp) throws IOException {
		String backupPath = backupType.equals(BackupType.SNAPSHOT) ?
				backupArgs.getSnapshotBackupFolderPathInTempSnapshotPath()
				: backupArgs.getIncrementalBackupFolderPathInTempSnapshotPath();
		Files.createDirectories(Paths.get(backupPath));
		try {

			// root folder name starts with "s" if this is a snapshot,
			// else it starts with "i", which means incremental backup.
			Path root = Paths.get(backupPath);
			for (Keyspace keyspace : keyspaces) {
				Path targetKeyspacePath = Paths.get(root.toString(), keyspace.getName());
				for (Table table : keyspace.getTables()) {
					addTableBackupEntriesToBackupFolder(backupType, backupTimestamp, targetKeyspacePath, table);
				}
			}
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	private void addTableBackupEntriesToBackupFolder(final String backupType,
													 final long backupTimestamp,
													 final Path targetKeyspacePath,
													 final Table table) throws IOException {
		// if some controls that performTableChecks do not pass, then don't add
		// any backup files for this table into temp backup folder. just return without failing.
		if (!performTableChecks(backupType, table)) {
			return;
		}
		Path sourceTableBackupFolder = (backupType.equals(BackupType.SNAPSHOT) ? getTableSnapshotPath(table) : getTableIncrementalBackupPath(table));
		Path targetTableBackupFolder = Paths.get(targetKeyspacePath.toString(), table.getTableName());
		Files.createDirectories(targetTableBackupFolder);
		for (File sourceBackupFile : sourceTableBackupFolder.toFile().listFiles()) {
			// do not add incremental backup files created after nodetool flush already completed.
			if (backupType.equals(BackupType.INCREMENTAL_BACKUP) && sourceBackupFile.lastModified() >= backupTimestamp) {
				continue;
			}
			if (sourceBackupFile.isDirectory()) {
				File targetIndexFolder = new File(Paths.get(targetTableBackupFolder.toString(), '.' + table.getTableName()).toString());
				targetIndexFolder.mkdirs();
				org.apache.commons.io.FileUtils.copyDirectory(sourceBackupFile, targetIndexFolder);
			} else {
				String targetBackupFilePath = Paths.get(targetTableBackupFolder.toString(), sourceBackupFile.getName()).toString();
				File targetBackupFile = new File(targetBackupFilePath);
				org.apache.commons.io.FileUtils.copyFile(sourceBackupFile, targetBackupFile);
			}
		}

	}

	/**
	 * This method performs some checks for sstables to backup on table level. If table doesn't
	 * have snapshot (or incremental backup) directory, it means the table does not have anything
	 * to backup. This method prints an info to the log in this case.
	 */
	private boolean performTableChecks(final String backupType, final Table table) {
		if (backupType.equals(BackupType.SNAPSHOT)) {
			// Snapshot directory exists of not
			if (tableSnapshotNotExists(table)) {
				getLogger().info(
						String.format(BackupFolderCreator.NO_SNAPSHOTS_FOUND_INFO,
								table.getKeyspace().getName(),
								table.getTableName()));
				return false;
			}
			// Snapshot file exists, but is it a directory check
			if (!checkTableSnapshotIsADir(table)) {
				getLogger().warn(
						String.format(BackupFolderCreator.SNAPSHOT_DIRECTORY_IS_NOT_A_DIRECTORY_WARNING,
								backupArgs.getBackupLabel(), table.getKeyspace().getName(), table.getTableName()));
				return false;
			}
		} else if (backupType.equals(BackupType.INCREMENTAL_BACKUP)) {
			// Checks if incremental backup directory exists in cassandra data path
			if (tableIncrementalBackupNotExists(table)) {
				getLogger().info(
						String.format(BackupFolderCreator.NO_INCREMENTAL_BACKUPS_FOUND_INFO,
								table.getKeyspace().getName(),
								table.getTableName()));
				return false;
			}

		}
		return true;
	}

	private boolean tableIncrementalBackupNotExists(final Table table) {
		Path tableIncrementalBackupPath = getTableIncrementalBackupPath(table);
		return !tableIncrementalBackupPath.toFile().exists() ||
				tableIncrementalBackupPath.toFile().listFiles().length == 0;
	}

	protected void readKeyspacesFromDisk(final BackupArgs backupArgs) {
		this.keyspaces = Keyspace.readKeyspacesFromDisk(backupArgs.getCassandraDataPath(), backupArgs.getKeyspaces());
	}

	/**
	 * Chekcs whether the system has enough space for backup file that will be created.
	 */
	protected void checkAvailableSpace(final long backupTimestamp) throws IOException, NodeAgentException {
		long backupSize = backupArgs.getBackupType().equals(BackupType.INCREMENTAL_BACKUP) ?
				getTotalIncrementalBackupSize() : getTotalSnapshotSize();
		long usableSpace = getUsableSpace(Paths.get(backupArgs.getTempBackupPath()));
		if (backupSize > usableSpace) {
			long usableSpaceInMb = (long) (((double) usableSpace) / Math.pow(10, 6));
			long backupSizeInMb = (long) (((double) backupSize) / Math.pow(10, 6));
			String message = String.format(BackupFolderCreator.NOT_ENOUGH_DISK_SPACE_ERROR,
					backupSizeInMb,
					1,
					backupSizeInMb,
					backupArgs.getTempBackupPath(),
					usableSpaceInMb);
			getLogger().error(message);
			throw new NotEnoughDiskSpaceException(message);
		}
	}

	protected long getTotalIncrementalBackupSize() throws IOException {
		long totalIncrementalBackupSize = 0;
		for (Keyspace keyspace : keyspaces) {
			for (Table table : keyspace.getTables()) {
				totalIncrementalBackupSize += tableIncrementalBackupSize(table);
			}
		}
		return totalIncrementalBackupSize;
	}

	private long tableIncrementalBackupSize(final Table table) throws IOException {
		if (tableIncrementalBackupNotExists(table)) {
			return 0;
		}
		return FileUtils.size(getTableIncrementalBackupPath(table));
	}

	protected long getUsableSpace(Path p) {
		while (p != null && !Files.exists(p)) {
			p = p.getParent();
		}
		if (p == null) {
			return 0;
		}
		File f = new File(p.toString());
		return f.getUsableSpace();
	}

	protected long getTotalSnapshotSize() throws IOException {
		long totalSnapshotSize = 0;
		for (Keyspace keyspace : keyspaces) {
			for (Table table : keyspace.getTables()) {
				totalSnapshotSize += tableSnapshotSize(table);
			}
		}
		return totalSnapshotSize;
	}


	private long tableSnapshotSize(final Table table) throws IOException {
		if (tableSnapshotNotExists(table)) {
			return 0;
		}
		return FileUtils.size(getTableSnapshotPath(table));
	}

	protected Logger getLogger() {
		return BackupFolderCreator.LOGGER;
	}

	protected boolean tableSnapshotNotExists(final Table table) {
		return !getTableSnapshotPath(table).toFile().exists();
	}

	protected boolean checkTableSnapshotIsADir(final Table table) {
		return getTableSnapshotPath(table).toFile().isDirectory();
	}

	protected Path getTableSnapshotPath(final Table table) {
		return Paths.get(table.getPath().toString(), Table.SNAPSHOTS_DIR_NAME, backupArgs.getBackupLabel());
	}

	protected Path getTableIncrementalBackupPath(final Table table) {
		return Paths.get(table.getPath().toString(), Table.INCREMENTAL_BACKUPS_DIR_NAME);
	}

	public void setBackupArgs(final BackupArgs backupArgs) {
		this.backupArgs = backupArgs;
		readKeyspacesFromDisk(backupArgs);
	}

	public List<Keyspace> getKeyspaces() {
		return keyspaces;
	}
}
