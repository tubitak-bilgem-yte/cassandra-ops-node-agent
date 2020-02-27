package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class RestoreRequest {
	private String clusterName;
	private String nodeName;
	private String batchRelation;
	private String parentRelation;
	private List<String> batchBackupFolders;
	private List<String> restoreKeyspaces;
}
