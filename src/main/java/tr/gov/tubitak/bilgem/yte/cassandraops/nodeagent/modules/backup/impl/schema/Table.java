package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema;

import java.nio.file.Path;

public class Table {
    public static final String SNAPSHOTS_DIR_NAME = "snapshots";
    public static final String INCREMENTAL_BACKUPS_DIR_NAME = "backups";
    private final Path path;
    private final Keyspace keyspace;
    private String tableName;

    public Table(final Path path, final Keyspace keyspace) {
        this.path = path;
        this.keyspace = keyspace;
        extractTableName(path);
    }

    private void extractTableName(final Path path) {
        String tableDir = path.getFileName().toString();
        this.tableName = tableDir.substring(0, tableDir.lastIndexOf("-"));

    }

    public String getTableName() {
        return tableName;
    }

    public Path getPath() {
        return path;
    }

    public Keyspace getKeyspace() {
        return keyspace;
    }


}
