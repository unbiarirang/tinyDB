package tinydb.index.bptree;

import tinydb.exec.consts.Constant;
import tinydb.file.Block;
import tinydb.record.Table;

public class BPTreeDir {
	private Table tb;
	private String filename;
	private BPTreePage contents;

	BPTreeDir(Block blk, Table tb) {
		this.tb = tb;
		filename = blk.fileName();
		contents = new BPTreePage(blk, tb);
	}

	public void close() {
		contents.close();
	}

	// search the block which is search key in
	public int search(Constant searchkey) {
		Block childblk = findChildBlock(searchkey);
		while (contents.getFlag() > 0) {
			contents.close();
			contents = new BPTreePage(childblk, tb);
			childblk = findChildBlock(searchkey);
		}
		return childblk.number();
	}

	public void makeNewRoot(DirEntry e) {
		Constant firstval = contents.getDataVal(0);
		int level = contents.getFlag();
		Block newblk = contents.split(0, level); // ie, transfer all the records
		DirEntry oldroot = new DirEntry(firstval, newblk.number());
		insertEntry(oldroot);
		insertEntry(e);
		contents.setFlag(level + 1);
	}

	// Insert new directory entry
	public DirEntry insert(DirEntry e) {
		if (contents.getFlag() == 0)
			return insertEntry(e);
		Block childblk = findChildBlock(e.dataVal());
		BPTreeDir child = new BPTreeDir(childblk, tb);
		DirEntry myentry = child.insert(e);
		child.close();
		return (myentry != null) ? insertEntry(myentry) : null;
	}

	private DirEntry insertEntry(DirEntry e) {
		int newslot = 1 + contents.findSlotBefore(e.dataVal());
		contents.insertDir(newslot, e.dataVal(), e.blockNumber());
		if (!contents.isFull())
			return null;

		int level = contents.getFlag();
		int splitpos = contents.getNumRecs() / 2;
		Constant splitval = contents.getDataVal(splitpos);
		Block newblk = contents.split(splitpos, level);
		return new DirEntry(splitval, newblk.number());
	}

	// get the next block
	private Block findChildBlock(Constant searchkey) {
		int slot = contents.findSlotBefore(searchkey);
		if (contents.getDataVal(slot + 1).equals(searchkey))
			slot++;
		int blknum = contents.getChildNum(slot);
		return new Block(filename, blknum);
	}
}
