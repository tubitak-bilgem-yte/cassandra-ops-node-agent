package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test;

import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args.BackupArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema.Keyspace;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema.Table;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.FileUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackupTestData {


	// base absolute path for tests
	private String homePath;
	// root directory in homePath for tests
	private String rootDirName;
	private BackupArgs backupArgsStub;
	private List<Keyspace> keyspaceStubList;

	public String getHomePath() {
		return homePath;
	}

	public String getRootDirName() {
		return rootDirName;
	}

	private BackupTestData() {
	}

	public static String randomFileName() {
		return randomUUID().toString();
	}

	public BackupArgs getBackupArgsStub() {
		return backupArgsStub;
	}

	public BackupArgs createRealBackupArgs() {
		BackupArgs backupArgs = new BackupArgs();
		backupArgs.setBackupType(getBackupArgsStub().getBackupType());
		backupArgs.setRelation(getBackupArgsStub().getRelation());
		backupArgs.setBackupLabel(getBackupArgsStub().getBackupLabel());
		backupArgs.setCassandraDataPath(getBackupArgsStub().getCassandraDataPath());
		backupArgs.setKeyspaces(getBackupArgsStub().getKeyspaces());
		backupArgs.setTempBackupPath(getBackupArgsStub().getTempBackupPath());
		return backupArgs;
	}


	public List<Keyspace> getKeyspaceStubList() {
		return keyspaceStubList;
	}


	public void deleteRoot() throws IOException {
		Path path = Paths.get(homePath, rootDirName);
		if (!path.toFile().exists()) {
			return;
		}
		FileUtils.delete(path);
	}

	public static class BackupTestDataBuilder {
		// represents keysapaces inside cassandraDataPath
		private final Map<String, TestKeyspacePath> keyspaces = new HashMap<>();
		// rootDirName is created inside homePath and all test files & directories are inside rootDirName.
		private String rootDirName = BackupTestData.randomFileName();
		// base absolute path for tests
		private String homePath = System.getProperty("user.home");
		// cassandraDataPath is created inside rootDirName.
		private TestPath cassandraDataPath;
		// tempBackupPath is created inside rootDirName.
		private TestPath tempBackupPath;
		private String backupLabel;
		private String backupType;
		private String relation;
		private String snapshotZipInTempBackupPath;
		private String incrementalBackupZipInTempBackupPath;
		// represents last added keyspace as test data
		private TestKeyspacePath lastAddedKeyspace;
		// represents last added table inside lastAddedKeyspace
		private TestTablePath lastAddedTable;

		public BackupTestDataBuilder() {
		}


		private String absolutePath(final String relativePath) {
			if (relativePath == null) {
				return null;
			}
			if (relativePath.trim().equals("")) {
				return "";
			}
			return Paths.get(homePath, rootDirName, relativePath).toString();
		}

		private String absoluteTablePath(final String tableName) {
			return Paths.get(lastAddedKeyspace.filePath, appendUUIDSuffixToTableName(tableName)).toString();
		}

		// cassandra appends uuid suffix to table directories. here it is simulated.
		private String appendUUIDSuffixToTableName(final String tableName) {
			String uuidSuffix = String.join("", randomUUID().toString().split("-"));
			return String.format("%s-%s", tableName, uuidSuffix);
		}

		public BackupTestDataBuilder addBackupLabel(final String backupLabel) {
			this.backupLabel = backupLabel;
			return this;
		}

		public BackupTestDataBuilder addBackupType(final String backupType) {
			this.backupType = backupType;
			return this;
		}

		public BackupTestDataBuilder addRelation(final String relation) {
			this.relation = relation;
			return this;
		}

		public BackupTestDataBuilder addCassandraDataPath(final String relativeCassandraDataPath,
														  final FileCreationType fileCreationType) {
			this.cassandraDataPath = new TestPath(absolutePath(relativeCassandraDataPath), fileCreationType);
			return this;
		}


		public BackupTestDataBuilder addTempBackupPath(final String relativeBackupPath, final FileCreationType fileCreationType) {
			this.tempBackupPath = new TestPath(absolutePath(relativeBackupPath), fileCreationType);
			return this;
		}

		public BackupTestDataBuilder addKeyspace(final String relativeFilePath, final FileCreationType fileCreationType) {
			if (cassandraDataPath == null) {
				throw new IllegalStateException("cassandraDataPath should be specified by calling " +
						"addCassandraDataPath before calling addKeyspace");
			}
			TestKeyspacePath keyspace = new TestKeyspacePath(
					Paths.get(cassandraDataPath.filePath, relativeFilePath).toString(),
					fileCreationType);
			keyspaces.put(relativeFilePath, keyspace);
			lastAddedKeyspace = keyspace;
			return this;
		}


		public BackupTestDataBuilder addTable(final String tableName) {
			if (lastAddedKeyspace == null) {
				throw new IllegalStateException(
						"A keyspace should be added with addKeyspace method before adding a table");
			}
			TestTablePath table = new TestTablePath(absoluteTablePath(tableName),
					FileCreationType.CREATE_AS_DIRECTORY);
			lastAddedKeyspace.tables.add(table);
			lastAddedTable = table;
			return this;
		}

		public BackupTestDataBuilder addSnapshotFile(final String fileName) {
			if (lastAddedTable == null) {
				throw new IllegalStateException(
						"A table should be added with addTable method before adding a snapshot file.");
			}
			lastAddedTable.snapshotFiles.add(fileName);
			lastAddedTable.snapshotFileSizes.add(0L);
			return this;
		}

		public BackupTestDataBuilder addSnapshotFile(final String fileName, final long size) {
			if (lastAddedTable == null) {
				throw new IllegalStateException(
						"A table should be added with addTable method before adding a snapshot file.");
			}
			lastAddedTable.snapshotFiles.add(fileName);
			lastAddedTable.snapshotFileSizes.add(size);
			return this;
		}

		public BackupTestDataBuilder addIncrementalBackupFile(final String fileName) {
			if (lastAddedTable == null) {
				throw new IllegalStateException(
						"A table should be added with addTable method before adding a snapshot file.");
			}
			lastAddedTable.incrementalBackupFiles.add(fileName);
			lastAddedTable.incrementalBackupFileSizes.add(0L);
			return this;
		}

		public BackupTestDataBuilder addIncrementalBackupFile(final String fileName, final long size) {
			if (lastAddedTable == null) {
				throw new IllegalStateException(
						"A table should be added with addTable method before adding a snapshot file.");
			}
			lastAddedTable.incrementalBackupFiles.add(fileName);
			lastAddedTable.incrementalBackupFileSizes.add(size);

			return this;
		}

		public BackupTestDataBuilder addSnapshotZipToTempBackupPath() {
			performChecksForTempZipCreation();
			this.snapshotZipInTempBackupPath = snapshotZipFileName();
			return this;
		}

		public BackupTestDataBuilder addIncrementalBackupZipToTempBackupPath() {
			performChecksForTempZipCreation();
			this.incrementalBackupZipInTempBackupPath = incrementalBackupZipFileName();
			return this;
		}

		private void performChecksForTempZipCreation() {
			if (backupLabel == null) {
				throw new IllegalStateException("backupLabel is not specified. Call addBackupLabel first.");
			}
			if (tempBackupPath == null) {
				throw new IllegalStateException("tempBackupPath is not specified. Call addTempBackupPath first.");
			}
			if (tempBackupPath.fileCreationType != FileCreationType.CREATE_AS_DIRECTORY) {
				throw new IllegalStateException("tempBackupPath should be created as an actual directory on disk. " +
						"Call addTempBackupPath with FileCreationType.CREATE_AS_DIRECTORY first.");
			}
		}


		public BackupTestData build() throws IOException {
			BackupTestData backupTestData = new BackupTestData();
			backupTestData.homePath = homePath;
			backupTestData.rootDirName = rootDirName;
			createFilesAndDirectories();
			createStubs(backupTestData);
			return backupTestData;
		}

		public BackupTestData buildSelectedRootDirectory(final String rootDiractoryName) throws IOException {
			BackupTestData backupTestData = new BackupTestData();
			backupTestData.homePath = homePath;
			backupTestData.rootDirName = rootDiractoryName;
			createFilesAndDirectories();
			createStubs(backupTestData);
			return backupTestData;
		}

		private String snapshotZipFileName() {
			return "s" + backupLabel + ".zip";
		}

		private String incrementalBackupZipFileName() {
			return "i" + backupLabel + ".zip";
		}

		private void createFile(final String path) throws IOException {
			Files.createDirectories(Paths.get(path).getParent());
			Files.createFile(Paths.get(path));
		}

		private void createFile(final String path, final long size) throws IOException {
			Files.createDirectories(Paths.get(path).getParent());
			RandomAccessFile randomBackupFileWithSize = new RandomAccessFile(path, "rw");
			randomBackupFileWithSize.setLength(size);
			randomBackupFileWithSize.close();
		}

		private void createStubs(final BackupTestData p) {
			p.backupArgsStub = mock(BackupArgs.class);
			when(p.backupArgsStub.getBackupLabel()).thenReturn(backupLabel);
			when(p.backupArgsStub.getKeyspaces()).thenReturn(new ArrayList<>(keyspaces.keySet()));
			when(p.backupArgsStub.getRelation()).thenReturn(relation);
			when(p.backupArgsStub.getBackupType()).thenReturn(backupType);

			if (cassandraDataPath != null) {
				when(p.backupArgsStub.getCassandraDataPath()).thenReturn(cassandraDataPath.filePath);
			}

			if (tempBackupPath != null) {
				when(p.backupArgsStub.getTempBackupPath()).thenReturn(tempBackupPath.filePath);
				if (backupLabel != null) {
					when(p.backupArgsStub.getSnapshotFolderName()).thenReturn(snapshotZipFileName());
					when(p.backupArgsStub.getIncrementalBackupFolderName()).thenReturn(incrementalBackupZipFileName());
					when(p.backupArgsStub.getSnapshotBackupFolderPathInTempSnapshotPath()).thenReturn(
							Paths.get(tempBackupPath.filePath, snapshotZipFileName()).toString());
					when(p.backupArgsStub.getIncrementalBackupFolderPathInTempSnapshotPath()).thenReturn(
							Paths.get(tempBackupPath.filePath,
									incrementalBackupZipFileName()).toString());
				}
			}

			p.keyspaceStubList = new ArrayList<>();
			for (Map.Entry<String, TestKeyspacePath> keyspaceEntry : keyspaces.entrySet()) {
				Keyspace keyspaceStub = mock(Keyspace.class);
				when(keyspaceStub.getName()).thenReturn(
						Paths.get(keyspaceEntry.getValue().filePath).getFileName().toString());
				when(keyspaceStub.getPath()).thenReturn(Paths.get(keyspaceEntry.getValue().filePath));
				List<Table> tableStubList = new ArrayList<>();
				for (TestTablePath table : keyspaceEntry.getValue().tables) {
					Table tableStub = mock(Table.class);
					Path tablePath = Paths.get(table.filePath);
					when(tableStub.getKeyspace()).thenReturn(keyspaceStub);
					when(tableStub.getTableName()).thenReturn(
							tablePath.getFileName().toString().split("-")[0]);
					when(tableStub.getPath()).thenReturn(
							tablePath);
					tableStubList.add(tableStub);
				}
				when(keyspaceStub.getTables()).thenReturn(tableStubList);
				p.keyspaceStubList.add(keyspaceStub);
			}
		}


		// for integration testing
		private void createFilesAndDirectories() throws IOException {
			createCassandraDataFilesAndDirectories();
			createTempBackupFilesAndDirectories();
		}

		private void createTempBackupFilesAndDirectories() throws IOException {
			if (tempBackupPath != null && tempBackupPath.fileCreationType != FileCreationType.DO_NOT_CREATE) {
				if (tempBackupPath.fileCreationType == FileCreationType.CREATE_AS_DIRECTORY) {
					Files.createDirectories(Paths.get(tempBackupPath.filePath));
					if (this.snapshotZipInTempBackupPath != null) {
						Files.createFile(Paths.get(tempBackupPath.filePath, snapshotZipInTempBackupPath));
					}
					if (this.incrementalBackupZipInTempBackupPath != null) {
						Files.createFile(Paths.get(tempBackupPath.filePath, incrementalBackupZipInTempBackupPath));
					}
				} else {
					createFile(tempBackupPath.filePath);
				}
			}
		}

		private void createCassandraDataFilesAndDirectories() throws IOException {
			if (cassandraDataPath != null && cassandraDataPath.fileCreationType != FileCreationType.DO_NOT_CREATE) {
				if (cassandraDataPath.fileCreationType == FileCreationType.CREATE_AS_DIRECTORY) {
					Files.createDirectories(Paths.get(cassandraDataPath.filePath));
					for (Map.Entry<String, TestKeyspacePath> entry : keyspaces.entrySet()) {
						TestKeyspacePath keyspace = entry.getValue();
						createCassandraKeyspaceFilesAndDirectories(keyspace);
					}
				} else if (cassandraDataPath.fileCreationType == FileCreationType.CREATE_AS_FILE) {
					createFile(cassandraDataPath.filePath);
				}
			}
		}


		private void createCassandraKeyspaceFilesAndDirectories(final TestKeyspacePath keyspace) throws IOException {
			if (keyspace.fileCreationType != FileCreationType.DO_NOT_CREATE) {
				if (keyspace.fileCreationType == FileCreationType.CREATE_AS_DIRECTORY) {
					Files.createDirectories(Paths.get(keyspace.filePath));
					for (TestTablePath table : keyspace.tables) {
						createTableFilesAndDirectories(table);
					}
				} else if (keyspace.fileCreationType == FileCreationType.CREATE_AS_FILE) {
					createFile(keyspace.filePath);
				}
			}
		}

		private void createTableFilesAndDirectories(final TestTablePath table) throws IOException {
			if (table.fileCreationType == FileCreationType.CREATE_AS_DIRECTORY) {
				Path tablePath = Paths.get(table.filePath);
				Files.createDirectories(tablePath);
				String tableSnapshotPath = Paths.get(tablePath.toString(),
						Table.SNAPSHOTS_DIR_NAME, backupLabel).toString();
				String tableIncrementalBackupPath = Paths.get(tablePath.toString(),
						Table.INCREMENTAL_BACKUPS_DIR_NAME).toString();
				int index = 0;
				for (String snapshotFile : table.snapshotFiles) {
					long size = table.snapshotFileSizes.get(index);
					createFile(Paths.get(tableSnapshotPath, snapshotFile).toString(), size);
					index++;
				}
				index = 0;
				for (String incrementalBackupFile : table.incrementalBackupFiles) {
					long size = table.incrementalBackupFileSizes.get(index);
					createFile(Paths.get(tableIncrementalBackupPath, incrementalBackupFile).toString(), size);
					index++;
				}
			}
		}

		public String getHomePath() {
			return homePath;
		}

		public BackupTestDataBuilder setHomePath(final String homePath) {
			this.homePath = homePath;
			return this;
		}

		public BackupTestDataBuilder setRootDirName(final String rootDirName) {
			this.rootDirName = rootDirName;
			return this;
		}

		private static class TestPath {
			final String filePath;
			final FileCreationType fileCreationType;

			public TestPath(final String filePath, final FileCreationType fileCreationType) {
				this.filePath = filePath;
				this.fileCreationType = fileCreationType;
			}
		}

		private static class TestKeyspacePath extends TestPath {
			final Set<TestTablePath> tables = new HashSet<>();

			public TestKeyspacePath(final String filePath, final FileCreationType fileCreationType) {
				super(filePath, fileCreationType);
			}
		}

		private static class TestTablePath extends TestPath {
			final List<String> snapshotFiles = new ArrayList<>();
			final List<Long> snapshotFileSizes = new ArrayList<>();
			final List<String> incrementalBackupFiles = new ArrayList<>();
			final List<Long> incrementalBackupFileSizes = new ArrayList<>();

			public TestTablePath(final String tableFileName, final FileCreationType fileCreationType) {
				super(tableFileName, fileCreationType);
			}
		}


	}
}
