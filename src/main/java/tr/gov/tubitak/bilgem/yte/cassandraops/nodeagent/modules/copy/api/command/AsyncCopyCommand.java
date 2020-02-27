package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.copy.api.command;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util.FileUtils;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.copy.api.args.CopyArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResult;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResultReceiver;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandStatus;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util.EnvironmentUtil;

import java.nio.file.Files;
import java.nio.file.Paths;

@Service
public class AsyncCopyCommand {
    private static final String COMMAND_NAME = "copy";
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncCopyCommand.class);
    private static final String STARTED_COPY_COMMAND = "Started running copy command (relation: %s).";
    private static final String COPY_SUCCESS_MESSAGE = "Copied %s to %s.";
    private static final String COPY_ERROR = "Something went wrong while copying %s to %s. Exception: %s. Message: %s.";
    private static final String MESSAGE_TEMPLATE = "%s Relation: %s";


    private EnvironmentUtil environmentUtil;


    /**
     * This method is called when a copy command is received from master.
     * It takes the backup files in temporary backup directory and copies them
     * to their final backup destination (e.g. an NFS partition). Since
     * the name of each bakcup file is unique, copy can be done simultaneously
     * in each node. Since copy operation may take some time, the method is
     * annoted as @Async.
     */
    @Async
    public void copy(final CopyArgs copyArgs,
                     final CommandResultReceiver resultReceiver) {
        getLogger().info(String.format(AsyncCopyCommand.STARTED_COPY_COMMAND, copyArgs.getCopyRelation()));
        String successMessage = String.format(AsyncCopyCommand.COPY_SUCCESS_MESSAGE, copyArgs.getSourcePath(), copyArgs.getDestinationPath());
        CommandResult commandResult = new CommandResult(AsyncCopyCommand.COMMAND_NAME,
                copyArgs.getCopyRelation(),
                CommandStatus.SUCCESS,
                successMessage,
                environmentUtil.getClusterName(),
                environmentUtil.getNodeName(),
                null);
        try {
            Files.createDirectories(Paths.get(copyArgs.getDestinationPath()).getParent());
            org.apache.commons.io.FileUtils.copyDirectory(Paths.get(copyArgs.getSourcePath()).toFile(),
                    Paths.get(copyArgs.getDestinationPath()).toFile());
            // If isDeleteSourceAfterCopy is set in copyArgs, it deletes the backup files in temporary direcotory
            if (copyArgs.isDeleteSourceAfterCopy()) {
                FileUtils.delete(Paths.get(copyArgs.getSourcePath()));
            }
            getLogger().info(String.format(AsyncCopyCommand.MESSAGE_TEMPLATE, successMessage, copyArgs.getCopyRelation()));
        } catch (final Exception e) {
            commandResult.setStatus(CommandStatus.ERROR);
            String errorMessage = String.format(
                    AsyncCopyCommand.COPY_ERROR,
                    copyArgs.getSourcePath(),
                    copyArgs.getDestinationPath(),
                    e.getClass().getName(),
                    e.getMessage());
            commandResult.setMessage(errorMessage);
            getLogger().error(String.format(AsyncCopyCommand.MESSAGE_TEMPLATE, errorMessage, copyArgs.getCopyRelation()), e);
        } finally {
            resultReceiver.sendCommandResult(commandResult);
        }
    }

    protected Logger getLogger() {
        return AsyncCopyCommand.LOGGER;
    }

    @Autowired
    protected void setEnvironmentUtil(final EnvironmentUtil environmentUtil) {
        this.environmentUtil = environmentUtil;
    }
}
