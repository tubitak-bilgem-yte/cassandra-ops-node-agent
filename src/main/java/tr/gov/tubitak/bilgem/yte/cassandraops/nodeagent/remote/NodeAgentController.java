package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.remote;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args.BackupArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.command.AsyncBackupCommand;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.copy.api.args.CopyArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.copy.api.command.AsyncCopyCommand;

import java.io.IOException;

@RestController
public class NodeAgentController {
    private NodeAgentCommandResultReceiver nodeAgentCommandResultReceiver;
    private AsyncBackupCommand asyncBackupCommand;
    private AsyncCopyCommand asyncCopyCommand;

    @PostMapping("backup")
    @ResponseStatus(HttpStatus.OK)
    public void backup(@RequestBody final BackupArgs backupArgs) throws IOException {
        asyncBackupCommand.backup(backupArgs, nodeAgentCommandResultReceiver);
    }

    @PostMapping("copy")
    public void copy(@RequestBody final CopyArgs copyArgs) {
        asyncCopyCommand.copy(copyArgs, nodeAgentCommandResultReceiver);
    }

    @Autowired
    protected void setNodeAgentCommandResultReceiver(final NodeAgentCommandResultReceiver nodeAgentCommandResultReceiver) {
        this.nodeAgentCommandResultReceiver = nodeAgentCommandResultReceiver;
    }

    @Autowired
    protected void setAsyncBackupCommand(final AsyncBackupCommand asyncBackupCommand) {
        this.asyncBackupCommand = asyncBackupCommand;
    }

    @Autowired
    protected void setAsyncCopyCommand(final AsyncCopyCommand asyncCopyCommand) {
        this.asyncCopyCommand = asyncCopyCommand;
    }
}
