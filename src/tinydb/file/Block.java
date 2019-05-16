package tinydb.file;

// A reference to a disk block
public class Block {
	private String filename;
	private int blknum;

	public Block(String filename, int blknum) {
		this.filename = filename;
		this.blknum = blknum;
	}

	public String fileName() {
		return filename;
	}

	public int number() {
		return blknum;
	}
}
