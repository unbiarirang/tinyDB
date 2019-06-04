package tinydb.record;

// An identifier for a record within a file.
public class RID {
	private int blknum;
	private int id;

	public RID(int blknum, int id) {
		this.blknum = blknum;
		this.id     = id;
	}
	
	public int blockNumber() {
		return blknum;
	}

	public int id() {
		return id;
	}

	public boolean equals(Object obj) {
		RID r = (RID) obj;
		return blknum == r.blknum && id==r.id;
	}
}
