package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.schema;

import org.junit.Test;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.test.TestConstants;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TableTest {

	@Test
	public void newTable_correctPath_success() {
		Keyspace keyspaceStub = mock(Keyspace.class);
		String backupFileName = "adet_hatali_lot_tekilurunler-8f59a87092f511e7bfaac9bfd74cec35";
		Path p = Paths.get(TestConstants.CASSANDRA_DATA_DIRECTORY_NAME, backupFileName);
		Table t = new Table(p, keyspaceStub);
		String expectecTableName = "adet_hatali_lot_tekilurunler";
		assertEquals(expectecTableName, t.getTableName());
	}
}
