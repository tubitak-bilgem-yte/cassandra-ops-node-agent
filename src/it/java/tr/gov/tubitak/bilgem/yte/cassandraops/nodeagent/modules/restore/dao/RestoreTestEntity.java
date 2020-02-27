package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.dao;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.util.UUID;

@Data
@Table("restore_test_table")
@AllArgsConstructor
@NoArgsConstructor
public class RestoreTestEntity {
    @PrimaryKeyColumn(name = "id", type = PrimaryKeyType.PARTITIONED, ordinal = 0)
    private Integer id;
    @Column("test_timestamp_column")
    private Long timestamp;
    @Column("test_name_column")
    private String name;
    @Column("test_uuid_column")
    private UUID uuid;
}
