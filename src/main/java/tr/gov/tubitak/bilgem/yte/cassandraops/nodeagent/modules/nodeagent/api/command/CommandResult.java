package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.nodeagent.api.command;

import java.util.Map;

public class CommandResult {
	private String commandName;
	private String relation;
	private String status;
	private String message;
	private String clusterName;
	private String nodeName;
	private Map<String, String> moreInfo;

	public CommandResult() {
	}

	public CommandResult(final String commandName,
						 final String relation,
						 final String status,
						 final String message,
						 final String clusterName,
						 final String nodeName,
						 final Map<String, String> moreInfo) {
		setCommandName(commandName);
		setRelation(relation);
		setStatus(status);
		setMessage(message);
		setClusterName(clusterName);
		setNodeName(nodeName);
		setMoreInfo(moreInfo);
	}

	public String getCommandName() {
		return commandName;
	}

	public void setCommandName(final String commandName) {
		this.commandName = commandName;
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(final String relation) {
		this.relation = relation;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(final String status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(final String message) {
		this.message = message;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(final String clusterName) {
		this.clusterName = clusterName;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(final String nodeName) {
		this.nodeName = nodeName;
	}

	public Map<String, String> getMoreInfo() {
		return moreInfo;
	}

	public void setMoreInfo(final Map<String, String> moreInfo) {
		this.moreInfo = moreInfo;
	}
}
