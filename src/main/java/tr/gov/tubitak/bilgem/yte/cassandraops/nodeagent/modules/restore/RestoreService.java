package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.command.AsyncBackupCommand;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.ExternalProcessFailedException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.CommandRunnerUtil;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResult;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResultReceiver;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandStatus;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity.RestoreRequest;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity.RestoreResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class RestoreService {
	private static final String BACKUP_DOES_NOT_EXISTS = "Backup with relation %s does not exists!";
	private static final String RESTORE_ERROR = "Restore operation was unsuccessful!";
	private static final String RESTORE_SUCCESS = "Backup with relation %s has been restored successfully!";
	private static final String RESTORE = "restore";
	private static final Logger LOGGER = LoggerFactory.getLogger(AsyncBackupCommand.class);
	private CommandRunnerUtil commandRunnerUtil;
	private RestoreResultSender restoreResultSender;

	@Async
	public void restore(final RestoreRequest restoreRequest) {
		RestoreResponse restoreResponse = new RestoreResponse();
		restoreResponse.setParentRelation(restoreRequest.getParentRelation());
		restoreResponse.setBatchRelation(restoreRequest.getBatchRelation());
		restoreResponse.setCurrentUniqueBackupName(getLastBackupName(restoreRequest.getBatchBackupFolders()));
		CommandResult commandResult = new CommandResult();
		commandResult.setCommandName(RestoreService.RESTORE);
		commandResult.setClusterName(restoreRequest.getClusterName());
		commandResult.setNodeName(restoreRequest.getNodeName());
		System.out.println("Restore Operation was Started.");
		try {
			restoreBackups(restoreRequest);
			commandResult.setMessage(String.format(RestoreService.RESTORE_SUCCESS, restoreRequest.getBatchRelation()));
			commandResult.setStatus(CommandStatus.SUCCESS);
			commandResult.setRelation(restoreRequest.getBatchRelation());
			getLogger().info(commandResult.getMessage());
		} catch (final IOException e) {
			commandResult.setMessage(String.format(RestoreService.BACKUP_DOES_NOT_EXISTS, restoreRequest.getBatchRelation()));
			commandResult.setStatus(CommandStatus.ERROR);
			getLogger().error(commandResult.getMessage(), e);
		} catch (final Exception e) {
			commandResult.setStatus(CommandStatus.ERROR);
			commandResult.setMessage(String.format("An unexpected error occured! Error: %s", e.getMessage()));
			getLogger().error(commandResult.getMessage(), e);
		} finally {
			restoreResponse.setCommandResult(commandResult);
			System.out.println("BatchRelation: " + restoreRequest.getBatchRelation() + " Parent Relation: " + restoreRequest.getParentRelation());
			restoreResultSender.sendRestoreResult(restoreResponse);

		}
	}

	public String getLastBackupName(final List<String> backupPaths) {
		String lastBackup = backupPaths.get(backupPaths.size() - 1);
		Pattern pattern = Pattern.compile("(ib|sb)[0-9]{10}");
		Matcher matcher = pattern.matcher(lastBackup);
		String lastBackupName = "";
		while (matcher.find()) {
			lastBackupName = matcher.group();
		}
		return lastBackupName;
	}

	public void restoreBackups(final RestoreRequest restoreRequest) throws InterruptedException, IOException, ExternalProcessFailedException {
		List<File> backupFilesList = restoreRequest.getBatchBackupFolders().stream()
				.map(File::new)
				.collect(Collectors.toList());

		restoreBackup(backupFilesList, restoreRequest.getRestoreKeyspaces());
	}

	public List<String> getBackupDirectoriesForGivenBackupName(final String bakupPath) throws IOException {
		return Files.list(Paths.get(bakupPath))
				.map(Path::toFile)
				.sorted(Comparator.comparing(File::lastModified))
				.map(File::getAbsolutePath)
				.collect(Collectors.toList());
	}

	private void restoreBackup(final List<File> backupFolders, final List<String> backupKeyspaces) throws InterruptedException, IOException, ExternalProcessFailedException {
		for (File backup : backupFolders) {
			getLogger().info(String.format("Restoring backup: %s", backup.getName()));
			List<File> keyspaces = Arrays.asList(backup.listFiles());
			keyspaces.sort(Comparator.comparing(File::getName));
			for (File keyspace : keyspaces) {
				if (backupKeyspaces.contains(keyspace.getName())) {
					restoreKeyspace(keyspace);
				}
			}
		}
	}

	private void restoreKeyspace(final File keyspace) throws InterruptedException, IOException, ExternalProcessFailedException {
		getLogger().info(String.format("Restoring keyspace: %s", keyspace.getName()));
		List<File> tables = Arrays.asList(keyspace.listFiles());
		tables.sort(Comparator.comparing(File::getName));
		for (File table : tables) {
			restoreTable(table);
		}
	}

	private void restoreTable(final File table) throws InterruptedException, IOException, ExternalProcessFailedException {
		getLogger().info(String.format("Restoring table: %s", table.getName()));
		commandRunnerUtil.restore(table.getAbsolutePath());
	}

	@Autowired
	public void setCommandRunnerUtil(final CommandRunnerUtil commandRunnerUtil) {
		this.commandRunnerUtil = commandRunnerUtil;
	}

	@Autowired
	public void setCommandResultSender(final CommandResultReceiver commandResultSender) {
		CommandResultReceiver commandResultSender1 = commandResultSender;
	}

	@Autowired
	public void setRestoreResultSender(final RestoreResultSender restoreResultSender) {
		this.restoreResultSender = restoreResultSender;
	}

	protected Logger getLogger() {
		return RestoreService.LOGGER;
	}

}
