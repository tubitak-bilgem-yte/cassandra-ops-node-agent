package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception;

import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;

public class ExternalProcessFailedException extends NodeAgentException {
	public ExternalProcessFailedException(final String message) {
		super(message);
	}
}
