package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.copy.api.args;

public class CopyArgs {
	private String sourcePath;
	private String destinationPath;
	private String copyRelation;
	private boolean deleteSourceAfterCopy;

	public String getSourcePath() {
		return sourcePath;
	}

	public void setSourcePath(final String sourcePath) {
		this.sourcePath = sourcePath;
	}

	public String getDestinationPath() {
		return destinationPath;
	}

	public void setDestinationPath(final String destinationPath) {
		this.destinationPath = destinationPath;
	}

	public String getCopyRelation() {
		return copyRelation;
	}

	public void setCopyRelation(final String copyRelation) {
		this.copyRelation = copyRelation;
	}

	public boolean isDeleteSourceAfterCopy() {
		return deleteSourceAfterCopy;
	}

	public void setDeleteSourceAfterCopy(final boolean deleteSourceAfterCopy) {
		this.deleteSourceAfterCopy = deleteSourceAfterCopy;
	}
}
