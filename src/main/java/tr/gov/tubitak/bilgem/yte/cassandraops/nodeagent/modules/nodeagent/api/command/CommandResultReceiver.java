package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command;

public interface CommandResultReceiver {
	String sendCommandResult(CommandResult result);
}
