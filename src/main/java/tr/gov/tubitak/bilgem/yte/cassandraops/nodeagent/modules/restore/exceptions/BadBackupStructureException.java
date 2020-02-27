package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.restore.exceptions;

public class BadBackupStructureException extends Exception {
	public BadBackupStructureException(final String message) {
		super(message);
	}
}
