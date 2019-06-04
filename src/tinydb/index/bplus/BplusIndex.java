package tinydb.index.bplus;

import static tinydb.consts.Types.*;
import tinydb.file.Block;
import tinydb.record.*;
import tinydb.exec.*;
import tinydb.exec.consts.Constant;
import tinydb.exec.consts.DoubleConstant;
import tinydb.exec.consts.FloatConstant;
import tinydb.exec.consts.IntConstant;
import tinydb.exec.consts.LongConstant;
import tinydb.exec.consts.StringConstant;
import tinydb.index.Index;

public class BplusIndex implements Index {
	private Table dirTi, leafTi;
	private BplusLeaf leaf = null;
	private Block rootblk;
	private Constant searchkey = null;
	private TableExec te = null;
	private int leafblknum;
	private int currentslot;

	public BplusIndex(String idxname, Schema leafsch) {

		String leaftbl = idxname + "leaf";
		leafTi = new Table(leaftbl, leafsch);

		Schema dirsch = new Schema();

		dirsch.add("block", leafsch);
		dirsch.add("dataval", leafsch);

		String dirtbl = idxname + "dir";
		dirTi = new Table(dirtbl, dirsch);
		rootblk = new Block(dirTi.fileName(), 0);
		currentslot = 0;

		BplusPage page = new BplusPage(rootblk, dirTi);
		if (page.getNumRecs() == 0) {
			int fldtype = dirsch.type("dataval");
			Constant minval;
			switch (fldtype) {
			case INTEGER:
				minval = new IntConstant(Integer.MIN_VALUE);
				break;
			case LONG:
				minval = new LongConstant(Long.MIN_VALUE);
				break;
			case FLOAT:
				minval = new FloatConstant(Float.MIN_VALUE);
				break;
			case DOUBLE:
				minval = new DoubleConstant(Double.MIN_VALUE);
				break;
			default:
				minval = new StringConstant("");
			}
			page.setFlag(0);
			page.insertDir(0, minval, 0);
		}
		page.close();
	}

	// find the leaf block of search key
	public void beforeFirst(Constant searchkey) {
		close();
		BplusDir root = new BplusDir(rootblk, dirTi);
		int blknum = root.search(searchkey);
		leafblknum = blknum;
		root.close();
		Block leafblk = new Block(leafTi.fileName(), blknum);
		leaf = new BplusLeaf(leafblk, leafTi, searchkey);
	}

	// get the data
	public boolean next() {
		while (currentslot <= leaf.contents.getNumRecs()) {
			if (leaf.contents.getDataVal(currentslot).compareTo(searchkey) == 0) {
				System.out.println("searchkey: " + searchkey.value() + " was found");
				leaf.setCurrentSlot(currentslot);
				currentslot++;
				return true;
			}
			currentslot++;
		}
		currentslot = 0;
		return false;
	}

	public RID getDataRid() {
		return leaf.getDataRid();
	}

	public void insert(Constant dataval, RID datarid) {
		beforeFirst(dataval);
		DirEntry e = leaf.insert(datarid);
		leaf.close();
		if (e == null)
			return;
		
		//if leaf block is full
		BplusDir root = new BplusDir(rootblk, dirTi);
		DirEntry e2 = root.insert(e);
		
		//if directory block is full
		if (e2 != null)
			root.makeNewRoot(e2);
		root.close();
	}

	public void delete(Constant dataval, RID datarid) {
		beforeFirst(dataval);
		leaf.delete(datarid);
		leaf.close();
	}

	public void close() {
		if (leaf != null)
			leaf.close();
	}

	public static int searchCost(int numblocks, int rpb) {
		return 1 + (int) (Math.log(numblocks) / Math.log(rpb));
	}

	// move to the head of leaf block
	public void moveToHead(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		beforeFirst(searchkey);
		te = new TableExec(leafTi);
		te.moveTo(leafblknum);
		System.out.println("searchkey: " + searchkey.value() + " moveToHead " + leafTi.fileName());
		leaf.setCurrentSlot(0);
	}

	public void modify(Constant oldval, Constant newval, RID datarid) {
		delete(oldval, datarid);
		insert(newval, datarid);
	}
}
