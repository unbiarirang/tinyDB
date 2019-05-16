package tinydb.record;

import static tinydb.file.Page.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import tinydb.file.*;

// Record in a block
public class Record {
	public static final int INUSE = 1;

	private Page 	p;
	private Block 	blk;
	private Table 	tb;
	private int 	recordsize;		// a record size
	private int 	currentid = -1; // current record id

	public Record(Table tb, Block blk) {
		this.tb = tb;
		this.blk = blk;
		this.p = new Page(blk);
		recordsize = tb.recordLength() + INT_SIZE;
		moveToFirst();
	}

	public boolean next() {
		return searchFor(INUSE);
	}

	/**
	 * flags(32bit): first 31 bits represent null value of each fields, 1 means the
	 * field is null. 
	 *   0      1            30      31
	 *   -----------------------------------
	 *  | fld1 | fld2 | ... | fld31 | INUSE | 
	 *   -----------------------------------
	 */
	private int fldIndexToFlag(int index) {
		return (1 << (31 - index));
	}

	public int getFlags() {
		int position = currentpos();
		return p.getInt(position);
	}

	public boolean getFlag(int flag) {
		return (getFlags() & flag) != 0;
	}

	public int getInt(String fldname) {
		int position = fieldpos(fldname);
		return p.getInt(position);
	}

	public long getLong(String fldname) {
		int position = fieldpos(fldname);
		return p.getLong(position);
	}

	public float getFloat(String fldname) {
		int position = fieldpos(fldname);
		return p.getFloat(position);
	}

	public double getDouble(String fldname) {
		int position = fieldpos(fldname);
		return p.getDouble(position);
	}

	public String getString(String fldname) {
		int position = fieldpos(fldname);
		return p.getString(position);
	}

	public void setFlags(int flags) {
		int position = currentpos();
		p.setInt(position, flags);
	}

	public void setFlag(int flag) {
		setFlags(getFlags() | flag);
	}

	public void removeFlags() {
		int position = currentpos();
		p.setInt(position, 0);
	}

	public void removeFlag(int flag) {
		int position = currentpos();
		p.setInt(position, getFlags() & ~flag);
	}

	public void setInt(String fldname, int val) {
		int position = fieldpos(fldname);
		p.setInt(position, val);
	}

	public void setLong(String fldname, long val) {
		int position = fieldpos(fldname);
		p.setLong(position, val);
	}

	public void setFloat(String fldname, float val) {
		int position = fieldpos(fldname);
		p.setFloat(position, val);
	}

	public void setDouble(String fldname, double val) {
		int position = fieldpos(fldname);
		p.setDouble(position, val);
	}

	public void setString(String fldname, String val) {
		int position = fieldpos(fldname);
		p.setString(position, val);
	}

	// Deletes the current record. Just mark the record as "deleted".
	public void delete() {
		removeFlag(INUSE);
	}

	// Insert a new empty record.
	public boolean insert() {
		currentid = -1;
		boolean found = searchForEmpty();
		if (found)
			setFlag(INUSE);
		return found;
	}

	// Insert a new record data to the page.
	public void insert(ArrayList<String> fldnames, ArrayList<Object> row) {
		Iterator<String> it1 = fldnames.iterator();
		Iterator<Object> it2 = row.iterator();

		while (it1.hasNext() && it2.hasNext()) {
			String fldname = it1.next();
			Object value = it2.next();

			if (value instanceof Integer)
				setInt(fldname, ((Integer) value).intValue());
			else if (value instanceof Long)
				setLong(fldname, ((Long) value).longValue());
			else if (value instanceof Float)
				setFloat(fldname, ((Float) value).floatValue());
			else if (value instanceof Double)
				setDouble(fldname, ((Double) value).doubleValue());
			else if (value instanceof String)
				setString(fldname, ((String) value).toString());
			else // case of null. just mask the field's null bit as 1
				setFlag(fldIndexToFlag(fldnames.indexOf(fldname)));
		}
		write();
	}

	public void close() {
		if (blk != null)
			blk = null;
	}

	public void write() {
		if (blk != null && p != null)
			p.write(blk);
	}

	// Print all attributes of a record.
	public void print() {
		ArrayList<String> fldnames = tb.fldnames();
		HashMap<String, String> fldtypes = tb.fldtypes();
		Iterator<String> it = fldnames.iterator();

		while (it.hasNext()) {
			String fldname = it.next();

			if (getFlag(fldIndexToFlag(fldnames.indexOf(fldname)))) // null
				System.out.print("null\t");
			else if (fldtypes.get(fldname).equals("Int"))
				System.out.print(getInt(fldname) + "\t");
			else if (fldtypes.get(fldname).equals("Long"))
				System.out.print(getLong(fldname) + "\t");
			else if (fldtypes.get(fldname).equals("Float"))
				System.out.print(getFloat(fldname) + "\t");
			else if (fldtypes.get(fldname).equals("Double"))
				System.out.print(getDouble(fldname) + "\t");
			else
				System.out.print(getString(fldname) + "\t");
		}
		System.out.println();
	}

	public void moveToFirst() {
		currentid = 0;
	}

	public void moveToId(int id) {
		currentid = id;
	}

	public int currentId() {
		return currentid;
	}

	private int currentpos() {
		return currentid * recordsize;
	}

	private int fieldpos(String fldname) {
		int offset = INT_SIZE + tb.offset(fldname);
		return currentpos() + offset;
	}

	private boolean isValidRecord() {
		return currentpos() + recordsize <= BLOCK_SIZE;
	}

	// Search for the first record that has specified flag.
	private boolean searchFor(int flag) {
		currentid++;
		while (isValidRecord()) {
			if (getFlag(flag))
				return true;
			currentid++;
		}
		return false;
	}

	// Search for the first empty record starting from the currentid.
	private boolean searchForEmpty() {
		currentid++;
		while (isValidRecord()) {
			if ((getFlags() & INUSE) == 0)
				return true;
			currentid++;
		}
		return false;
	}
}
