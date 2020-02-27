package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception;

import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;

public class NotEnoughDiskSpaceException extends NodeAgentException {
	public NotEnoughDiskSpaceException(final String message) {
		super(message);
	}
}
