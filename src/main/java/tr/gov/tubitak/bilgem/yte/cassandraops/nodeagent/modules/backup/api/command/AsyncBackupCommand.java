package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args.BackupArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.BackupCleaner;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.BackupFolderCreator;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.CommandRunnerUtil;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.FileUtils;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResult;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResultReceiver;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandStatus;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util.EnvironmentUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;

@Service
public class AsyncBackupCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncBackupCommand.class);
    private static final String COMMAND_NAME = "backup";
    private static final String BACKUP_RUNNING_INFO = "Started running backup command (backupType: %s, relation: %s)";
    private static final String CREATED_BACKUP_STARTED_COMPRESSING_INFO = "Created backup. Started compressing.";
    private static final String CLEANUP_INFO = "Comressing finished. Cleaning up if parameter is given.";
    private static final String SNAPSHOT_CREATED_INFO = "Snapshot %s successfully created";
    private static final String SNAPSHOT_CREATION_ERROR = "An error happend while taking snapshot. Creating error result to be sent to master.";
    private static final String SNAPSHOT_EXCEPTION_TEMPLATE = "Could not create snapshot. Exception: %s, Message: %s";
    private static final String DURATION_INFO = "It took around %d minutes to complete";
    private static final String FINISHED_RUNNING_INFO = "Finished running.";

    private CommandRunnerUtil commandRunnerUtil;
    private EnvironmentUtil environmentUtil;

    @Async
    public void backup(final BackupArgs backupArgs, final CommandResultReceiver resultReceiver) throws IOException {
        long startTime = currentTimeMillis();
        getLogger().info(String.format(AsyncBackupCommand.BACKUP_RUNNING_INFO,
                backupArgs.getBackupType(),
                backupArgs.getRelation()));
        CommandResult result;
        Map<String, String> moreInfo = new HashMap<>();


        moreInfo.put("backupLabel", backupArgs.getBackupLabel());
        try {
            backupArgs.validate();
            // Depending on the type of backup, it runs the nodetool command for
            // incremental or snapshot backup
            long backupCreationTime = runBackupOperation(backupArgs);
            BackupFolderCreator backupFolderCreator = new BackupFolderCreator(backupArgs);
            // Copies all of the SSTables into a temporary folder
            backupFolderCreator.createTempBackupFolder(backupCreationTime);
            // Deletes the backups inside cassandra data path
            cleanup(backupArgs, backupCreationTime);
            long backupSize = getBackupSize(backupArgs);
            moreInfo.put("backupType", backupArgs.getBackupType());
            moreInfo.put("backupSize", "" + backupSize);
            moreInfo.put("backupFilePath",
                    backupArgs.getBackupType().equals(BackupType.SNAPSHOT)
                            ? backupArgs.getSnapshotBackupFolderPathInTempSnapshotPath()
                            : backupArgs.getIncrementalBackupFolderPathInTempSnapshotPath());
            String message = String.format(AsyncBackupCommand.SNAPSHOT_CREATED_INFO, backupArgs.getBackupLabel());
            result = getCommandResult(backupArgs, moreInfo, CommandStatus.SUCCESS, message);
        } catch (final Exception e) {
            getLogger().error(AsyncBackupCommand.SNAPSHOT_CREATION_ERROR, e);
            String message = String.format(AsyncBackupCommand.SNAPSHOT_EXCEPTION_TEMPLATE, e.getClass().getName(), e.getMessage());
            result = getCommandResult(backupArgs, moreInfo, CommandStatus.ERROR, message);
        }
        // Send the successfull completion message to master
        resultReceiver.sendCommandResult(result);
        getLogger().info(AsyncBackupCommand.FINISHED_RUNNING_INFO);
        long endTime = currentTimeMillis();
        long durationInMinutes = (long) Math.floor((endTime - startTime) / (1000.0 * 60.0));
        getLogger().info(String.format(AsyncBackupCommand.DURATION_INFO, durationInMinutes));
    }

    public Set<String> getBackupRelationsFromBackupPath(final String backupPath) throws IOException {
        return Files.list(Paths.get(backupPath))
                .map(x -> x.toString().split("_")[1])
                .collect(Collectors.toSet());
    }


    private CommandResult getCommandResult(final BackupArgs backupArgs, final Map<String, String> moreInfo, final String commandStatus, final String message) {
        CommandResult result;
        result = new CommandResult(AsyncBackupCommand.COMMAND_NAME,
                backupArgs.getRelation(),
                commandStatus,
                message,
                environmentUtil.getClusterName(),
                environmentUtil.getNodeName(),
                moreInfo);
        return result;
    }

    /**
     * Runs the necessary command on a process with the help of cassandra's nodetool utility.
     * Returns current time to measure the time it took to run the command
     */
    private long runBackupOperation(final BackupArgs backupArgs) throws IOException, InterruptedException, NodeAgentException {
        if (backupArgs.getBackupType().equals(BackupType.SNAPSHOT)) {
            commandRunnerUtil.snapshot(backupArgs.getBackupLabel(), backupArgs.getKeyspaces());
        } else {
            commandRunnerUtil.flush();
        }
        //commandRunnerUtil.compact();
        getLogger().info(AsyncBackupCommand.CREATED_BACKUP_STARTED_COMPRESSING_INFO);
        return System.currentTimeMillis();
    }

    private void cleanup(final BackupArgs backupArgs, final long backupCreationTime)
            throws InterruptedException, IOException, NodeAgentException {
        if (!backupArgs.isCleanOldBackups()) {
            return;
        }
        // if this was a snapshot, delete the snapshot files since they are now compressed
        getLogger().info(AsyncBackupCommand.CLEANUP_INFO);
        if (backupArgs.getBackupType().equals(BackupType.SNAPSHOT)) {
            commandRunnerUtil.clearSnapshots(backupArgs.getBackupLabel(), backupArgs.getKeyspaces());
        }
        // if this was a snapshot or an incremental backup operation, there will always be
        // incremental backup files and a snapshot always contains data of incremental backups.
        // so always delete incremental backup files independent of backup type.
        BackupCleaner backupCleaner = new BackupCleaner(backupArgs);
        backupCleaner.deleteIncrementalBackupFiles(backupCreationTime);
    }

    private long getBackupSize(final BackupArgs backupArgs) throws IOException {
        long totalBackupSize;
        if (backupArgs.getBackupType().equals(BackupType.SNAPSHOT)) {
            totalBackupSize = FileUtils.size(Paths.get(backupArgs.getSnapshotBackupFolderPathInTempSnapshotPath()));
        } else {
            totalBackupSize = FileUtils.size(Paths.get(backupArgs.getIncrementalBackupFolderPathInTempSnapshotPath()));
        }
        return totalBackupSize;
    }

    @Autowired
    protected void setCommandRunnerUtil(final CommandRunnerUtil commandRunnerUtil) {
        this.commandRunnerUtil = commandRunnerUtil;
    }

    @Autowired
    protected void setEnvironment(final EnvironmentUtil environmentUtil) {
        this.environmentUtil = environmentUtil;
    }

    protected Logger getLogger() {
        return AsyncBackupCommand.LOGGER;
    }
}
