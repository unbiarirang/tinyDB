package tinydb.index.btree;

import static tinydb.consts.Types.*;
import static tinydb.file.Page.*;

import tinydb.file.Block;
import tinydb.file.Page;
import tinydb.record.*;
import tinydb.exec.*;
import tinydb.exec.consts.Constant;
import tinydb.exec.consts.DoubleConstant;
import tinydb.exec.consts.FloatConstant;
import tinydb.exec.consts.IntConstant;
import tinydb.exec.consts.LongConstant;
import tinydb.exec.consts.StringConstant;

public class BTreePage {
	private Block currentblk;
	private Table tb;
	private int slotsize;
	private Page contents;

	public BTreePage(Block currentblk, Table tb) {
		this.currentblk = currentblk;
		this.tb = tb;
		slotsize = tb.recordLength();
		contents = new Page();
		contents.read(currentblk);
	}

	public int findSlotBefore(Constant searchkey) {
		int slot = 0;
		String s = (String) getDataVal(slot).value();
		int a = getDataVal(slot).compareTo(searchkey);
		int num = getNumRecs();
		while (slot <= getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
			slot++;
		return slot - 1;
//		return slot;
	}

	public void close() {
		currentblk = null;
	}

	public boolean isFull() {
		return slotpos(getNumRecs() + 1) >= BLOCK_SIZE;
	}

	public Block split(int splitpos, int flag) {
		Block newblk = appendNew();
		BTreePage newpage = new BTreePage(newblk, tb);
		transferRecs(splitpos, newpage);
		newpage.setFlag(flag);
		newpage.close();
		return newblk;
	}

	public Constant getDataVal(int slot) {
		return getVal(slot, "dataval");
	}

	public int getFlag() {
		return contents.getInt(0);
	}

	public void setFlag(int val) {
		contents.setInt(0, val);
	}

	public Block appendNew() {
		return new Page().append(tb.fileName());
	}

	// Methods called only by BTreeDir

	/**
	 * Returns the block number stored in the index record at the specified slot.
	 * 
	 * @param slot the slot of an index record
	 * @return the block number stored in that record
	 */
	public int getChildNum(int slot) {
		return getInt(slot, "block");
	}

	/**
	 * Inserts a directory entry at the specified slot.
	 * 
	 * @param slot   the slot of an index record
	 * @param val    the dataval to be stored
	 * @param blknum the block number to be stored
	 */
	public void insertDir(int slot, Constant val, int blknum) {
		insert(slot);
		setVal(slot, "dataval", val);
		setInt(slot, "block", blknum);
		contents.write(currentblk);
	}

	// Methods called only by BTreeLeaf

	/**
	 * Returns the dataRID value stored in the specified leaf index record.
	 * 
	 * @param slot the slot of the desired index record
	 * @return the dataRID value store at that slot
	 */
	public RID getDataRid(int slot) {
		return new RID(getInt(slot, "block"), getInt(slot, "id"));
	}

	/**
	 * Inserts a leaf index record at the specified slot.
	 * 
	 * @param slot the slot of the desired index record
	 * @param val  the new dataval
	 * @param rid  the new dataRID
	 */
	public void insertLeaf(int slot, Constant val, RID rid) {
		insert(slot);
		setVal(slot, "dataval", val);
		setInt(slot, "block", rid.blockNumber());
		setInt(slot, "id", rid.id());
		contents.write(currentblk);
		int num = getNumRecs();
		
	}

	/**
	 * Deletes the index record at the specified slot.
	 * 
	 * @param slot the slot of the deleted index record
	 */
	public void delete(int slot) {
		for (int i = slot + 1; i < getNumRecs(); i++)
			copyRecord(i, i - 1);
		setNumRecs(getNumRecs() - 1);
		contents.write(currentblk);
	}

	/**
	 * Returns the number of index records in this page.
	 * 
	 * @return the number of index records in this page
	 */
	public int getNumRecs() {
		return contents.getInt(INT_SIZE);
	}

	// Private methods

	private int getInt(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return contents.getInt(pos);
	}

	private long getLong(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return contents.getLong(pos);
	}

	private float getFloat(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return contents.getFloat(pos);
	}

	private double getDouble(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return contents.getDouble(pos);
	}

	private String getString(int slot, String fldname) {
		int pos = fldpos(slot, fldname);
		return contents.getString(pos);
	}

	private Constant getVal(int slot, String fldname) {
		int type = tb.schema().type(fldname);
		if (type == INTEGER)
			return new IntConstant(getInt(slot, fldname));
		else if (type == LONG)
			return new LongConstant(getLong(slot, fldname));
		else if (type == FLOAT)
			return new FloatConstant(getFloat(slot, fldname));
		else if (type == DOUBLE)
			return new DoubleConstant(getDouble(slot, fldname));
		else
			return new StringConstant(getString(slot, fldname));
	}

//	private void setInt(int slot, String fldname, int val) {
//		int pos = fldpos(slot, fldname);
//		Page p = new Page();
//		p.read(currentblk);
//		p.setInt(pos, val);
//	}
//	
//	private void setLong(int slot, String fldname, long val) {
//		int pos = fldpos(slot, fldname);
//		Page p = new Page();
//		p.read(currentblk);
//		p.setLong(pos, val);
//	}
//
//	private void setFloat(int slot, String fldname, float val) {
//		int pos = fldpos(slot, fldname);
//		Page p = new Page();
//		p.read(currentblk);
//		p.setFloat(pos, val);
//	}
//
//	private void setDouble(int slot, String fldname, double val) {
//		int pos = fldpos(slot, fldname);
//		Page p = new Page();
//		p.read(currentblk);
//		p.setDouble(pos, val);
//	}
//
//	private void setString(int slot, String fldname, String val) {
//		int pos = fldpos(slot, fldname);
//		Page p = new Page();
//		p.read(currentblk);
//		p.setString(pos, val);
//	}
	private void setInt(int slot, String fldname, int val) {
		int pos = fldpos(slot, fldname);
		contents.setInt(pos, val);
	}
	
	private void setLong(int slot, String fldname, long val) {
		int pos = fldpos(slot, fldname);
		contents.setLong(pos, val);
	}

	private void setFloat(int slot, String fldname, float val) {
		int pos = fldpos(slot, fldname);
		contents.setFloat(pos, val);
	}

	private void setDouble(int slot, String fldname, double val) {
		int pos = fldpos(slot, fldname);
		contents.setDouble(pos, val);
	}

	private void setString(int slot, String fldname, String val) {
		int pos = fldpos(slot, fldname);
		contents.setString(pos, val);
	}
	
	
	private void setVal(int slot, String fldname, Constant val) {
		int type = tb.schema().type(fldname);
		if (type == INTEGER)
			setInt(slot, fldname, (Integer) val.value());
		else if (type == LONG)
			setLong(slot, fldname, (Long) val.value());
		else
			setString(slot, fldname, (String) val.value());
	}

//	private void setNumRecs(int n) {
//		Page p = new Page();
//		p.read(currentblk);
//		p.setInt(INT_SIZE, n);
//	}
	private void setNumRecs(int n) {
		contents.setInt(INT_SIZE, n);
//		contents.append(tb.fileName());
	}


	private void insert(int slot) {
		for (int i = getNumRecs(); i > slot; i--)
			copyRecord(i - 1, i);
		setNumRecs(getNumRecs() + 1);
	}

	private void copyRecord(int from, int to) {
		Schema sch = tb.schema();
		for (String fldname : sch.fields())
			setVal(to, fldname, getVal(from, fldname));
	}

	private void transferRecs(int slot, BTreePage dest) {
		int destslot = 0;
		while (slot < getNumRecs()) {
			dest.insert(destslot);
			Schema sch = tb.schema();
			for (String fldname : sch.fields())
				dest.setVal(destslot, fldname, getVal(slot, fldname));
			delete(slot);
			destslot++;
		}
	}

	private int fldpos(int slot, String fldname) {
		int offset = tb.offset(fldname);
		return slotpos(slot) + offset;
	}

	private int slotpos(int slot) {
		return INT_SIZE + INT_SIZE + (slot * slotsize);
	}
}
