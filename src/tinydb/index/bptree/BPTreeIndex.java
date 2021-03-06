package tinydb.index.bptree;

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

public class BPTreeIndex implements Index {
	private Table dirTi, leafTi;
	private BPTreeLeaf leaf = null;
	private Block rootblk;
	private Constant searchkey = null;
	private TableExec te = null;
	private int leafblknum;
	private int currentslot;
	private String relation = null;
	private boolean isOr = false;
	private boolean gtpivot = false; // true -> directly return
	private boolean epivot = false; // true -> ignore

	public BPTreeIndex(String idxname, Schema leafsch) {

		String leaftbl = idxname + "leaf";
		leafTi = new Table(leaftbl, leafsch);

		Schema dirsch = new Schema();

		dirsch.add("block", leafsch);
		dirsch.add("dataval", leafsch);

		String dirtbl = idxname + "dir";
		dirTi = new Table(dirtbl, dirsch);
		rootblk = new Block(dirTi.fileName(), 0);
		currentslot = 0;

		BPTreePage page = new BPTreePage(rootblk, dirTi);
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
		BPTreeDir root = new BPTreeDir(rootblk, dirTi);
		int blknum = root.search(searchkey);
		leafblknum = blknum;
		root.close();
		Block leafblk = new Block(leafTi.fileName(), blknum);
		leaf = new BPTreeLeaf(leafblk, leafTi, searchkey);
	}

	// get the data, stop searching,when return false;
	public boolean next() {
		if (epivot) {// && !isOr) { // the value was found already, do not need to traverse anymore
			System.out.println("stop traversing^^!(e)");
			currentslot = 0;
			return false;
		}

		while (currentslot <= leaf.contents.getNumRecs()) {

			if (gtpivot) { // the rest of values are all greater than searchkey, directly return true
				System.out.println("found directly^^!(ge)");
				leaf.setCurrentSlot(currentslot);
				currentslot++;
				return true;
			} else if ((relation.contentEquals(">=") && leaf.contents.getDataVal(currentslot).compareTo(searchkey) >= 0)
					|| (relation.contentEquals(">")
							&& leaf.contents.getDataVal(currentslot).compareTo(searchkey) > 0)) {
				System.out.println("searchkey: " + searchkey.value() + " was found!(gt)");
				leaf.setCurrentSlot(currentslot);
				gtpivot = true;
				currentslot++;
				return true;
			} else if ((relation.contentEquals("=")
					&& leaf.contents.getDataVal(currentslot).compareTo(searchkey) == 0)) {
				System.out.println("searchkey: " + searchkey.value() + " was found!(e)");
				leaf.setCurrentSlot(currentslot);
				currentslot++;
				epivot = true;
				return true;
			} else if ((relation.contentEquals("<=") && leaf.contents.getDataVal(currentslot).compareTo(searchkey) <= 0)
					|| (relation.contentEquals("<")
							&& leaf.contents.getDataVal(currentslot).compareTo(searchkey) < 0)) {
				System.out.println("searchkey: " + searchkey.value() + " was found!(lt)");
				leaf.setCurrentSlot(currentslot);
				currentslot++;
				return true;
			}

			// the rest of values are all greater than searchkey, do not need to traverse
			// anymore
			if (relation.contentEquals("<=") || (relation.contentEquals("<"))) {
				System.out.println("stop traversing^^!(lt)");
				break;
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

		// if leaf block is full
		BPTreeDir root = new BPTreeDir(rootblk, dirTi);
		DirEntry e2 = root.insert(e);

		// if directory block is full
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
	public void moveToHead(Constant searchkey, String relation, boolean isOr) {
		close();
		this.searchkey = searchkey;
		this.relation = relation;
		this.isOr = isOr;
		beforeFirst(searchkey);
		te = new TableExec(leafTi);
		te.moveTo(leafblknum);
//		System.out.println("searchkey: " + searchkey.value() + " moveToHead " + leafTi.fileName());
		leaf.setCurrentSlot(0);
	}

	public void modify(Constant oldval, Constant newval, RID datarid) {
		delete(oldval, datarid);
		insert(newval, datarid);
	}
}
