package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore;


import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.entity.RestoreResponse;

public interface RestoreResultSender {

	String sendRestoreResult(RestoreResponse result);
}
