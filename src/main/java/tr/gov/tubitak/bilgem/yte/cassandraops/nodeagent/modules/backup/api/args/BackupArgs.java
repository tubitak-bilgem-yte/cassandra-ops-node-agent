package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.*;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class BackupArgs {

	private static final Logger LOGGER = LoggerFactory.getLogger(BackupArgs.class);
	private static final String CASSANDRA_DATA_PATH_ARG_NAME = "cassandraDataPath";
	private static final String TEMP_BACKUP_PATH_ARG_NAME = "tempBackupPath";
	private static final String BACKUP_LABEL_ARG_NAME = "backupLabel";
	private static final String BACKUP_TYPE_ARG_NAME = "backupType";
	private static final String KEYSPACES_ARG_NAME = "keyspaces";
	private static final String RELATION_ARG_NAME = "relation";
	private static final String MANDATORY_ARGUMENT_MISSING_ERROR = "Exactly one value should be specified for argument %s but found none.";
	private static final String AT_LEAST_ONE_ARGUMENT_ERROR = "At least one value should be defined for %s parameter.";
	private static final String CHILD_ERROR = "%s should not be a child of %s";
	private static final String SNAPSHOT_EXISTS_ERROR = "There is already a snapshot zip file %s at %s";
	private static final String INCREMENTAL_EXISTS_ERROR = "There is already an incremental backup zip file %s at %s";
	private static final String PATH_MISSING_ERROR = "Path does not exist: %s";
	private static final String PATH_IS_NOT_A_DIRECTORY = "Path is not a directory: %s";
	private String cassandraDataPath;
	private String tempBackupPath;
	private String backupLabel;         // random string to make each copy or backup operation unique
	private String backupType;          // snapshot, incremental_backup
	private List<String> keyspaces;
	private String relation;
	private boolean cleanOldBackups;

	public BackupArgs() {
	}

	/**
	 * Returns absolute path of Cassandra node's data path
	 */
	public String getCassandraDataPath() {
		return cassandraDataPath;
	}

	public void setCassandraDataPath(final String cassandraDataPath) {
		this.cassandraDataPath = cassandraDataPath != null ? cassandraDataPath.trim() : null;
	}

	/**
	 * Returns list of keyspaces for which snapshots will be copied.
	 */
	public List<String> getKeyspaces() {
		return keyspaces;
	}

	public void setKeyspaces(final List<String> keyspaces) {
		if (keyspaces != null) {
			this.keyspaces = keyspaces.stream().map(s -> s.trim()).collect(Collectors.toList());
		}
	}

	public boolean isCleanOldBackups() {
		return cleanOldBackups;
	}

	public String getBackupLabel() {
		return backupLabel;
	}

	public void setBackupLabel(final String backupLabel) {
		this.backupLabel = backupLabel != null ? backupLabel.trim() : null;
	}

	public String getBackupType() {
		return backupType;
	}

	public void setBackupType(final String backupType) {
		this.backupType = backupType;
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(final String relation) {
		this.relation = relation != null ? relation.trim() : null;
	}

	public String getTempBackupPath() {
		return tempBackupPath;
	}

	public void setTempBackupPath(final String tempBackupPath) {
		this.tempBackupPath = tempBackupPath != null ? tempBackupPath.trim() : null;
	}

	public String getSnapshotBackupFolderPathInTempSnapshotPath() {
		return Paths.get(getTempBackupPath(), getSnapshotFolderName()).toString();
	}

	public String getSnapshotFolderName() {
		return "s" + getBackupLabel();
	}

	public String getIncrementalBackupFolderPathInTempSnapshotPath() {
		return Paths.get(getTempBackupPath(), getIncrementalBackupFolderName()).toString();
	}

	public String getIncrementalBackupFolderName() {
		return "i" + getBackupLabel();
	}

	public void validate() throws NodeAgentException {
		performBasicValidations();
		performAdvancedValidations();

	}

	/**
	 * Basic validations do not do any disk operations for validation.
	 * Examples of basic validations of params: mandatory checks, parent
	 * child checks, name format checks, etc.
	 */
	private void performBasicValidations() throws NodeAgentException {
		checkMandatoryArgument(BackupArgs.CASSANDRA_DATA_PATH_ARG_NAME, getCassandraDataPath());
		checkMandatoryArgument(BackupArgs.TEMP_BACKUP_PATH_ARG_NAME, getTempBackupPath());
		// check tempBackupPath is not child of cassandraDataPath
		checkPathIsNotChildOfOtherPath(getTempBackupPath(),
				BackupArgs.TEMP_BACKUP_PATH_ARG_NAME,
				getCassandraDataPath(),
				BackupArgs.CASSANDRA_DATA_PATH_ARG_NAME);
		// check if at least one keyspace is provided
		checkMandatoryArgument(BackupArgs.KEYSPACES_ARG_NAME, getKeyspaces());
		checkMandatoryArgument(BackupArgs.BACKUP_LABEL_ARG_NAME, getBackupLabel());
		checkMandatoryArgument(BackupArgs.BACKUP_TYPE_ARG_NAME, getBackupType());
		checkMandatoryArgument(BackupArgs.RELATION_ARG_NAME, getRelation());
	}

	/**
	 * Advanced validations are mostly disk system validations.
	 */
	private void performAdvancedValidations() throws NodeAgentException {
		validateCassandraDataPath();
		validateTempSnapshotPath();
		validateKeyspaces();
	}


	private void checkMandatoryArgument(final String argName, final String argValue) throws ArgumentValueMandatoryException {
		if (argValue == null || argValue.isEmpty()) {
			String message = String.format(BackupArgs.MANDATORY_ARGUMENT_MISSING_ERROR
					,
					argName);
			getLogger().error(message);
			throw new ArgumentValueMandatoryException(message);
		}
	}

	private void checkMandatoryArgument(final String argName, final List<String> argValue) throws ArgumentValueMandatoryException {
		if (argValue == null || argValue.size() < 1) {
			String message = String.format(BackupArgs.AT_LEAST_ONE_ARGUMENT_ERROR,
					argName);
			getLogger().error(message);
			throw new ArgumentValueMandatoryException(message);
		}
	}

	private void checkPathIsNotChildOfOtherPath(final String path,
												final String pathArgName,
												final String otherPath,
												final String otherPathArgName) throws IncorrectPathException {
		if (path.equals(otherPath) || path.startsWith(otherPath)) {
			String message = String.format(BackupArgs.CHILD_ERROR,
					pathArgName, otherPathArgName);
			getLogger().error(message);
			throw new IncorrectPathException(message);
		}
	}

	/**
	 * Checks if there is a valid path for cassandra to put its data, and also whether this path is a
	 * directory
	 */
	private void validateCassandraDataPath() throws NodeAgentException {
		checkPathExists(getCassandraDataPath());
		checkPathIsDirectory(getCassandraDataPath());
	}

	/**
	 * Checks if there is an already a zip file with given zip file name. The main thing that is
	 * being checked in here is that the zip file should not have same relation and time with an existing
	 * backup zip file. This is a very unlikely scneraio which happens when two random relation string
	 * collides with each other with a very low probability, or if user gives the relation by hand, and it
	 * already exists, and these should happen in the same minute with another backup file.
	 */
	private void validateTempSnapshotPath() throws NodeAgentException {
		checkBackupZipFilesNotExists();
	}

	/**
	 * Checks each given keyspace exists, and they are directories.
	 */
	private void validateKeyspaces() throws NodeAgentException {
		checkKeyspacePathsExists(getCassandraDataPath(), getKeyspaces());
		checkKeyspacePathsAreDirectories(getCassandraDataPath(), getKeyspaces());
	}

	protected void checkBackupZipFilesNotExists() throws NodeAgentException {
		if (getBackupType().equals(BackupType.SNAPSHOT)) {
			Path tempSnapshotBackupFolder = Paths.get(getSnapshotBackupFolderPathInTempSnapshotPath());
			if (tempSnapshotBackupFolder.toFile().exists()) {
				String message = String.format(BackupArgs.SNAPSHOT_EXISTS_ERROR,
						getSnapshotFolderName(),
						getTempBackupPath());
				getLogger().error(message);
				throw new PathAlreadyExistsException(message);
			}
		} else if (getBackupType().equals(BackupType.INCREMENTAL_BACKUP)) {

			Path tempIncrementalBackup = Paths.get(getIncrementalBackupFolderPathInTempSnapshotPath());
			if (Files.exists(tempIncrementalBackup)) {
				String message = String.format(BackupArgs.INCREMENTAL_EXISTS_ERROR,
						getIncrementalBackupFolderName(),
						getTempBackupPath());
				getLogger().error(message);
				throw new PathAlreadyExistsException(message);
			}
		}

	}

	private void checkKeyspacePathsAreDirectories(final String cassandraDataPathStr, final List<String> keyspaceNames)
			throws NodeAgentException {
		for (String s : keyspaceNames) {
			String p = Paths.get(cassandraDataPathStr, s).toString();
			checkPathIsDirectory(p);
		}
	}

	private void checkKeyspacePathsExists(final String cassandraDataPathStr, final List<String> keyspacesArgList) throws NodeAgentException {
		for (String keyspace : keyspacesArgList) {
			String keyspacePath = Paths.get(cassandraDataPathStr, keyspace).toString();
			checkPathExists(keyspacePath);
		}
	}

	protected Logger getLogger() {
		return BackupArgs.LOGGER;
	}

	protected void checkPathExists(final String pathStr) throws NodeAgentException {
		Path p = Paths.get(pathStr);
		if (!Files.exists(p)) {
			String message = String.format(BackupArgs.PATH_MISSING_ERROR, pathStr);
			getLogger().error(message);
			throw new PathDoesNotExistException(message);
		}
	}

	protected void checkPathIsDirectory(final String pathStr) throws NodeAgentException {
		Path p = Paths.get(pathStr);
		if (!Files.isDirectory(p)) {
			String message = String.format(BackupArgs.PATH_IS_NOT_A_DIRECTORY, pathStr);
			getLogger().error(message);
			throw new NotADirectoryException(message);
		}
	}

}