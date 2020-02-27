package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity;

import lombok.Getter;
import lombok.Setter;
import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command.CommandResult;

@Getter
@Setter
public class RestoreResponse {
	private String parentRelation;
	private String batchRelation;
	private String currentUniqueBackupName;
	private CommandResult commandResult;
}
