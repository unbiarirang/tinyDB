package tinydb.index.btree;

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

public class BTreeIndex implements Index {
	private Table dirTi, leafTi;
	private BTreeLeaf leaf = null;
	private Block rootblk;

	public BTreeIndex(String idxname, Schema leafsch) {
		// deal with the leaves
		String leaftbl = idxname + "leaf";
		leafTi = new Table(leaftbl, leafsch);
		// deal with the directory
		Schema dirsch = new Schema();
		
		dirsch.add("block", leafsch);
		dirsch.add("dataval", leafsch);

		String dirtbl = idxname + "dir";
		dirTi = new Table(dirtbl, dirsch);
		rootblk = new Block(dirTi.fileName(), 0);
		
		BTreePage page = new BTreePage(rootblk, dirTi);
		if (page.getNumRecs() == 0) {
			// insert initial directory entry
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

	/**
	 * Traverses the directory to find the leaf block corresponding to the specified
	 * search key. The method then opens a page for that leaf block, and positions
	 * the page before the first record (if any) having that search key. The leaf
	 * page is kept open, for use by the methods next and getDataRid.
	 * 
	 * @see tinydb.index.Index#beforeFirst(tinydb.exec.consts.query.Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		close();
		BTreeDir root = new BTreeDir(rootblk, dirTi);
		int blknum = root.search(searchkey);
		root.close();
		Block leafblk = new Block(leafTi.fileName(), blknum);
		leaf = new BTreeLeaf(leafblk, leafTi, searchkey);
	}

	/**
	 * Moves to the next leaf record having the previously-specified search key.
	 * Returns false if there are no more such leaf records.
	 * 
	 * @see tinydb.index.Index#next()
	 */
	public boolean next() {
		return leaf.next();
	}

	/**
	 * Returns the dataRID value from the current leaf record.
	 * 
	 * @see tinydb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		return leaf.getDataRid();
	}

	public void insert(Constant dataval, RID datarid) {
		beforeFirst(dataval);
		DirEntry e = leaf.insert(datarid);
		leaf.close();
		if (e == null)
			return;
		BTreeDir root = new BTreeDir(rootblk, dirTi);
		DirEntry e2 = root.insert(e);
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

	@Override
	public void moveToHead(Constant searchkey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void modify(Constant oldval, Constant newval, RID datarid) {
		// TODO Auto-generated method stub
		
	}
}
