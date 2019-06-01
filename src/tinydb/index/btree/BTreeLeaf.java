package tinydb.index.btree;

import tinydb.exec.consts.Constant;
import tinydb.file.Block;
import tinydb.record.*;

public class BTreeLeaf {
	private Table tb;
	private Constant searchkey;
	private BTreePage contents;
	private int currentslot;

	public BTreeLeaf(Block blk, Table tb, Constant searchkey) {
		this.tb = tb;
		this.searchkey = searchkey;
		contents = new BTreePage(blk, tb);
		currentslot = contents.findSlotBefore(searchkey);
	}

	public void close() {
		contents.close();
	}

	public boolean next() {
		currentslot++;
		if (currentslot >= contents.getNumRecs())
			return tryOverflow();
		else if (contents.getDataVal(currentslot).equals(searchkey))
			return true;
		else
			return tryOverflow();
	}

	public RID getDataRid() {
		return contents.getDataRid(currentslot);
	}

	public void delete(RID datarid) {
		while (next())
			if (getDataRid().equals(datarid)) {
				contents.delete(currentslot);
				return;
			}
	}

	public DirEntry insert(RID datarid) {
		// bug fix: If the page has an overflow page
		// and the searchkey of the new record would be lowest in its page,
		// we need to first move the entire contents of that page to a new block
		// and then insert the new record in the now-empty current page.
		if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchkey) > 0) {
			Constant firstval = contents.getDataVal(0);
			Block newblk = contents.split(0, contents.getFlag());
			currentslot = 0;
			contents.setFlag(-1);
			contents.insertLeaf(currentslot, searchkey, datarid);
			return new DirEntry(firstval, newblk.number());
		}

		currentslot++;
		contents.insertLeaf(currentslot, searchkey, datarid);
		if (!contents.isFull())
			return null;
		// else page is full, so split it
		Constant firstkey = contents.getDataVal(0);
		Constant lastkey = contents.getDataVal(contents.getNumRecs() - 1);
		if (lastkey.equals(firstkey)) {
			// create an overflow block to hold all but the first record
			Block newblk = contents.split(1, contents.getFlag());
			contents.setFlag(newblk.number());
			return null;
		} else {
			int splitpos = contents.getNumRecs() / 2;
			Constant splitkey = contents.getDataVal(splitpos);
			if (splitkey.equals(firstkey)) {
				// move right, looking for the next key
				while (contents.getDataVal(splitpos).equals(splitkey))
					splitpos++;
				splitkey = contents.getDataVal(splitpos);
			} else {
				// move left, looking for first entry having that key
				while (contents.getDataVal(splitpos - 1).equals(splitkey))
					splitpos--;
			}
			Block newblk = contents.split(splitpos, -1);
			return new DirEntry(splitkey, newblk.number());
		}
	}

	private boolean tryOverflow() {
		Constant firstkey = contents.getDataVal(0);
		int flag = contents.getFlag();
		if (!searchkey.equals(firstkey) || flag < 0)
			return false;
		contents.close();
		Block nextblk = new Block(tb.fileName(), flag);
		contents = new BTreePage(nextblk, tb);
		currentslot = 0;
		return true;
	}
}
