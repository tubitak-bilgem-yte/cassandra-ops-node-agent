package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.Flush;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.args.BackupType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.dao.RestoreTestEntity;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.BackupTestData;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.FileCreationType;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants.BACKUP_DIRECTORY;
import static tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants.HOME_DIRECTORY;

@PropertySource("classpath:test-application.properties")
@Configuration
public class RestoreServiceTestConfiguration extends AbstractCassandraConfiguration {

    private static final String KEYSPACE_CREATION_QUERY = "CREATE KEYSPACE IF NOT EXISTS nodeagent_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}";
    private static final String KEYSPACE_ACTIVATE_QUERY = "USE nodeagent_test";

    @Value("${nodeagent.backup.cassandraDataPath}")
    private String testCassandraDataPath;

    private String backupFolder;

    @Autowired
    private CassandraAdminOperations cassandraAdminOperations;

    @Autowired
    private Session session;

    public RestoreServiceTestConfiguration() {
        final Cluster cluster = Cluster.builder()
                .addContactPoints("localhost")
                .withPort(9042)
                .build();
        final Session session = cluster.connect();
        session.execute(RestoreServiceTestConfiguration.KEYSPACE_CREATION_QUERY);
        session.execute(RestoreServiceTestConfiguration.KEYSPACE_ACTIVATE_QUERY);

    }

    @PostConstruct
    public void initiliazeTable() throws IOException {
        cassandraAdminOperations.createTable(true, CqlIdentifier.of("restore_test_table"), RestoreTestEntity.class, new HashMap<>());
        insertDataToTestTable();
        createDirectoryStructure();
        copySSTables();

    }

    @PreDestroy
    public void deleteKeyspace() {
        session.execute("drop keyspace nodeagent_test");
    }

    private void insertDataToTestTable() throws IOException {
        for (int i = 0; i < 10; i++) {
            RestoreTestEntity currentEntity = new RestoreTestEntity(i, System.currentTimeMillis(), "test_name", UUIDs.random());
            cassandraAdminOperations.insert(currentEntity);
        }
        Flush flush = new Flush();
        flush.execute(new NodeProbe("localhost", 9042));
    }


    private void createDirectoryStructure() throws IOException {
        String homePath = Paths.get(HOME_DIRECTORY, BACKUP_DIRECTORY).toString();
        new BackupTestData.BackupTestDataBuilder()
                .setHomePath(homePath)
                .setRootDirName("cluster1")
                .addCassandraDataPath("test_backup_name", FileCreationType.CREATE_AS_DIRECTORY)
                .addBackupLabel(TestConstants.BACKUP_LABEL)
                .addBackupType(BackupType.SNAPSHOT)
                .addKeyspace("nodeagent_test", FileCreationType.CREATE_AS_DIRECTORY)
                .build();

        backupFolder = Paths.get(HOME_DIRECTORY, BACKUP_DIRECTORY, "cluster1", "test_backup_name", "nodeagent_test", "restore_test_table").toString();

    }

    private void copySSTables() {
        Path keyspacePath = Paths.get(testCassandraDataPath, "nodeagent_test");
        List<File> tables = Arrays.stream(keyspacePath.toFile().listFiles())
                .sorted(Comparator.comparing(File::lastModified))
                .collect(Collectors.toList());

        File lastModifiedFile = tables.get(tables.size() - 1);

        Arrays.stream(lastModifiedFile.listFiles())
                .filter(File::isFile)
                .forEach(file -> {
                    try {
                        FileUtils.copyFile(file, new File(Paths.get(backupFolder, file.getName()).toString()));
                    } catch (final IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    @Override
    protected String getKeyspaceName() {
        return "nodeagent_test";
    }

    @Bean
    @Override
    public CassandraClusterFactoryBean cluster() {
        CassandraClusterFactoryBean cluster =
                new CassandraClusterFactoryBean();
        cluster.setContactPoints("localhost");
        cluster.setPort(9042);
        return cluster;
    }

    @Bean
    public CassandraConverter converter() {
        return new MappingCassandraConverter(cassandraMapping());
    }

    @Bean
    @Override
    public CassandraMappingContext cassandraMapping() {
        return new CassandraMappingContext();
    }
}
