package tinydb.index.bptree;

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

public class BPTreePage {
	private Block currentblk;
	private Table tb;
	private int slotsize;
	private Page contents;

	public BPTreePage(Block currentblk, Table tb) {
		this.currentblk = currentblk;
		this.tb = tb;
		slotsize = tb.recordLength();
		contents = new Page();
		contents.read(currentblk);
	}

	// the place where will data be inserted;
	public int findSlotBefore(Constant searchkey) {
		int slot = 0;
		while (slot <= getNumRecs() && getDataVal(slot).compareTo(searchkey) < 0)
			slot++;
		return slot - 1;
	}

	public void close() {
		currentblk = null;
	}

	public boolean isFull() {
		return slotpos(getNumRecs() + 1) >= BLOCK_SIZE;
	}

	// when the block is full,split the original block to two block
	public Block split(int splitpos, int flag) {
		Block newblk = appendNew();
		BPTreePage newpage = new BPTreePage(newblk, tb);
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

	public int getChildNum(int slot) {
		return getInt(slot, "block");
	}

	// output directory data to disk
	public void insertDir(int slot, Constant val, int blknum) {
		insert(slot);
		setVal(slot, "dataval", val);
		setInt(slot, "block", blknum);
		contents.write(currentblk);
	}

	public RID getDataRid(int slot) {
		return new RID(getInt(slot, "block"), getInt(slot, "id"));
	}

	// output leaf data to disk
	public void insertLeaf(int slot, Constant val, RID rid) {
		insert(slot);
		setVal(slot, "dataval", val);
		setInt(slot, "block", rid.blockNumber());
		setInt(slot, "id", rid.id());
		contents.write(currentblk);
		int num = getNumRecs();

	}

	public void delete(int slot) {
		for (int i = slot + 1; i < getNumRecs(); i++)
			copyRecord(i, i - 1);
		setNumRecs(getNumRecs() - 1);
		contents.write(currentblk);
	}

	// number of record
	public int getNumRecs() {
		return contents.getInt(INT_SIZE);
	}

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
		String tblname = tb.tableName();
		String after = tblname.substring(tblname.length() - 4);
		// if leaf table
//		int test = (Integer) ((Long) val.value()).intValue();
		if (val.value() instanceof Long) {
			if (type == INTEGER)
				setInt(slot, fldname, (Integer) ((Long) val.value()).intValue());
			else if (type == LONG)
				setLong(slot, fldname, (Long) val.value());
		} else if (val.value() instanceof Double) {
			if (type == INTEGER)
				setInt(slot, fldname, (Integer) ((Double) val.value()).intValue());
			else if (type == LONG)
				setLong(slot, fldname, (Long) ((Double) val.value()).longValue());
			else if (type == FLOAT)
				setFloat(slot, fldname, (Float) ((Double) val.value()).floatValue());
			else if (type == DOUBLE)
				setDouble(slot, fldname, (Double) val.value());
		} else { // if directory table
			if (type == INTEGER)
				setInt(slot, fldname, (Integer) val.value());
			else if (type == LONG)
				setLong(slot, fldname, (Long) val.value());
			else if (type == FLOAT)
				setFloat(slot, fldname, (Float) val.value());
			else if (type == DOUBLE)
				setDouble(slot, fldname, (Double) val.value());
			else
				setString(slot, fldname, (String) val.value());
		}
	}

	private void setNumRecs(int n) {
		contents.setInt(INT_SIZE, n);
	}

	// make the spare space in specified slot
	private void insert(int slot) {
		for (int i = getNumRecs(); i >= slot; i--)
			copyRecord(i, i + 1);
		setNumRecs(getNumRecs() + 1);
	}

	private void copyRecord(int from, int to) {
		Schema sch = tb.schema();
		for (String fldname : sch.fields())
			setVal(to, fldname, getVal(from, fldname));
	}

	// transfer the data to dest Block
	private void transferRecs(int slot, BPTreePage dest) {
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
