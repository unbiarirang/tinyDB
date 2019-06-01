package tinydb.index.btree;

import tinydb.exec.consts.Constant;

public class DirEntry {
	private Constant dataval;
	private int blocknum;

	public DirEntry(Constant dataval, int blocknum) {
		this.dataval = dataval;
		this.blocknum = blocknum;
	}

	public Constant dataVal() {
		return dataval;
	}

	public int blockNumber() {
		return blocknum;
	}
}
