package tinydb.metadata;

// Statistical information about a table
// Holds number of blocks, number of records and number of distinct values
public class StatInfo {
	private int numBlocks;
	private int numRecs;

	public StatInfo(int numblocks, int numrecs) {
		this.numBlocks = numblocks;
		this.numRecs = numrecs;
	}

	public int blocksAccessed() {
		return numBlocks;
	}

	public int recordsOutput() {
		return numRecs;
	}

	// Estimation of the number of distinct values of an relation.
	public int distinctValues(String fldname) {
		return 1 + (numRecs / 3);
	}
}
