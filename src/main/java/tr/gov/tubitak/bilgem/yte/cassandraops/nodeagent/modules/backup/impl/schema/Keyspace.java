package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a Cassandra keyspace.
 */
public class Keyspace {
    private final String name;
    private final Path path;
    private final List<Table> tables;

    public Keyspace(final Path path) {
        this.path = path;
        this.name = path.getFileName().toString();
        tables = new ArrayList<>();
    }

    public static List<Keyspace> readKeyspacesFromDisk(final String cassandraDataPath, final List<String> keyspaceNames) {
        List<Keyspace> keyspaces = new ArrayList<>();
        for (String keyspace : keyspaceNames) {
            Path keyspacePath = Paths.get(cassandraDataPath, keyspace);
            Keyspace k = new Keyspace(keyspacePath);
            keyspaces.add(k);
            Keyspace.readTablesOfKeyspaceFromDisk(k);
        }
        return keyspaces;
    }

    private static void readTablesOfKeyspaceFromDisk(final Keyspace keyspace) {
        for (File file : keyspace.getPath().toFile().listFiles()) {
            if (file.isDirectory()) {
                Table t = new Table(file.toPath(), keyspace);
                keyspace.addTable(t);
            }
        }
    }

    public String getName() {
        return name;
    }

    public Path getPath() {
        return path;
    }

    private void addTable(final Table t) {
        tables.add(t);
    }

    public List<Table> getTables() {
        return tables;
    }

}
