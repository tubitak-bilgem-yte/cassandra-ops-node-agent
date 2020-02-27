package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception;

import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;

public class PathAlreadyExistsException extends NodeAgentException {
	public PathAlreadyExistsException(final String message) {
		super(message);
	}
}
