package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.util;

import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.api.args.BackupArgs;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema.Keyspace;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class BackupCleaner {
    private List<Keyspace> keyspaces;

    public BackupCleaner(final BackupArgs backupArgs) {
        readKeyspacesFromDisk(backupArgs);
    }

    public void deleteIncrementalBackupFiles(final long deleteBeforeTimestamp) throws IOException {
        for (Keyspace keyspace : keyspaces) {
            for (Table table : keyspace.getTables()) {
                FileUtils.cleanPath(getTableIncrementalBackupPath(table), deleteBeforeTimestamp);
            }
        }
    }

    protected void readKeyspacesFromDisk(final BackupArgs backupArgs) {
        this.keyspaces = Keyspace.readKeyspacesFromDisk(backupArgs.getCassandraDataPath(), backupArgs.getKeyspaces());
    }

    private Path getTableIncrementalBackupPath(final Table table) {
        return Paths.get(table.getPath().toString(), Table.INCREMENTAL_BACKUPS_DIR_NAME);
    }
}
