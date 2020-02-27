package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception.ExternalProcessFailedException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.util.EnvironmentUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Runs nodetool commands (nodetool snapshot, nodetool clearsnapshot, etc.) and
 * logs the outputs of these commands.
 */
@Service
public class CommandRunnerUtil {
    //TODO: remove bats
    private static final String SNAPSHOT_COMMAND_TEMPLATE = "nodetool %s snapshot %s --tag %s";
    private static final String CLEAR_SNAPSHOT_COMMAND_TEMPLATE = "nodetool %s clearsnapshot -t %s -- %s";
    private static final String FLUSH_COMMAND_TEMPLATE = "nodetool %s flush";
    private static final String COMPACT_COMMAND_TEMPLATE = "nodetool %s compact";
    private static final String COMMAND_FAILED_ERROR = "Command '%s' failed. See logs.";
    private static final String RUNNING_COMMAND_INFO = "Started running command: '%s'";
    private static final String SSTABLELOADER_COMMAND_TEMPLATE = "sstableloader -d %s -u %s -pw %s %s";
    private static final String LOGIN_FLAGS = "-u %s -pw %s";
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandRunnerUtil.class);
    private EnvironmentUtil environmentUtil;


    /**
     * Runs the snapshot command with user name, password, keyspaces that will be bakcked-up and snapshot label
     */
    public void snapshot(final String snapshotLabel, final List<String> keyspaceList) throws IOException, InterruptedException, NodeAgentException {
        String keyspaces = String.join(" ", keyspaceList);
        String command = String.format(CommandRunnerUtil.SNAPSHOT_COMMAND_TEMPLATE, getUserNameAndPassword(), keyspaces, snapshotLabel);
        command = getCassandraBinDir() + command;
        runCommand(command, false);
    }

    /**
     * Runs clearsnapshot command to delete the snapshot files of given snapshotLabel and of given keyspaceList.
     */
    public void clearSnapshots(final String snapshotLabel, final List<String> keyspaceList) throws InterruptedException, IOException, ExternalProcessFailedException {
        String keyspaces = String.join(" ", keyspaceList);
        String command = String.format(CommandRunnerUtil.CLEAR_SNAPSHOT_COMMAND_TEMPLATE, getUserNameAndPassword(), snapshotLabel, keyspaces);
        command = getCassandraBinDir() + command;
        runCommand(command, false);
    }

    /**
     * Flushes the node's memtables to be written to disk as sstables.
     * Incremental backup works as follows: First of all, incremental_backups option
     * in cassandra.yaml should be set to true. When this option is set, cassandra
     * writes each newly created sstable file to its default location and to backups
     * folder of each table (by using hardlinks). nodetool doesn't have a default command
     * for taking an incremental backup. What is done in here is that flush is called
     * through nodetool to write the latest memtables inside backups folders of tables.
     */
    public void flush() throws InterruptedException, IOException, ExternalProcessFailedException {
        String command = String.format(CommandRunnerUtil.FLUSH_COMMAND_TEMPLATE, getUserNameAndPassword());
        command = getCassandraBinDir() + command;
        runCommand(command, false);
    }

    public void restore(final String tablePath) throws InterruptedException, IOException, ExternalProcessFailedException {
        String command = String.format(CommandRunnerUtil.SSTABLELOADER_COMMAND_TEMPLATE, environmentUtil.getNodePublicIpAddress(), environmentUtil.getCassandraUserName(), environmentUtil.getCassandraPassword(), tablePath);
        command = getCassandraBinDir() + command;
        runCommand(command, true);
    }

    public void compact() throws InterruptedException, IOException, ExternalProcessFailedException {
        String command = String.format(CommandRunnerUtil.COMPACT_COMMAND_TEMPLATE, getUserNameAndPassword());
        command = getCassandraBinDir() + command;
        runCommand(command, false);
    }

    @Autowired
    public void setEnvironment(final EnvironmentUtil environmentUtil) {
        this.environmentUtil = environmentUtil;
    }

    private String getCassandraBinDir() {
        String cassandraBinDir = environmentUtil.getCassandraBinDir();
        if (cassandraBinDir == null || cassandraBinDir.trim().isEmpty()) {
            return "";
        }
        return cassandraBinDir;
    }

    private String getUserNameAndPassword() {
        String userName = environmentUtil.getCassandraUserName();
        String password = environmentUtil.getCassandraPassword();
        if (userName == null || userName.trim().isEmpty() || password == null || password.trim().isEmpty()) {
            return "";
        }
        return String.format(CommandRunnerUtil.LOGIN_FLAGS, userName, password);
    }

    private void runCommand(final String command, final boolean printLiveOutput) throws IOException, InterruptedException, ExternalProcessFailedException {
        getLogger().info(String.format(CommandRunnerUtil.RUNNING_COMMAND_INFO, command));
        Process p = Runtime.getRuntime().exec(command);
        if (printLiveOutput) {
            BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = is.readLine()) != null) {
                getLogger().info(line);
            }
        }
        p.waitFor();
        String output = readCommandOutput(p);
        if (p.exitValue() == 0) {
            getLogger().info(output);
        } else {
            getLogger().info(output);
            throw new ExternalProcessFailedException(String.format(CommandRunnerUtil.COMMAND_FAILED_ERROR, command));
        }
    }

    private String readCommandOutput(final Process p) throws IOException {
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = getReader(p)) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
            return output.toString();
        }
    }

    private Logger getLogger() {
        return CommandRunnerUtil.LOGGER;
    }

    private BufferedReader getReader(final Process p) {
        BufferedReader reader;
        if (p.exitValue() == 0) {
            reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        } else {
            reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        }
        return reader;
    }
}
