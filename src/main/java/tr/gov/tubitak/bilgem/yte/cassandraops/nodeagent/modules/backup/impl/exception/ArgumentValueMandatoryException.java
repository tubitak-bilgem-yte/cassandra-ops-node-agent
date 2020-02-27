package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup.impl.exception;

import tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.exception.NodeAgentException;

public class ArgumentValueMandatoryException extends NodeAgentException {
	public ArgumentValueMandatoryException(final String message) {
		super(message);
	}
}
