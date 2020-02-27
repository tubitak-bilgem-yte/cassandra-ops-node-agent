package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception;

import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;

public class IncorrectPathException extends NodeAgentException {
	public IncorrectPathException(final String message) {
		super(message);
	}
}
