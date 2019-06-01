package tinydb.exec;

import tinydb.exec.consts.Constant;
import tinydb.index.Index;

public class IndexJoinExec implements Exec {
	private Exec e;
	private TableExec te; // the data table
	private Index idx;
	private String joinfield;

	public IndexJoinExec(Exec e, TableExec te, Index idx, String joinfield) {
		this.e = e;
		this.te = te;
		this.idx = idx;
		this.joinfield = joinfield;

		moveToHead();
	}

	// Exec methods //

	public void moveToHead() {
		e.moveToHead();
		e.next();
		resetIndex();
	}

	// Move to the next index record, if possible.
    // Otherwise, it moves to the next LHS record and the first index record.
	public boolean next() {
		while (true) {
			if (idx.next()) {
				te.moveToRid(idx.getDataRid());
				return true;
			}
			if (!e.next())
				return false;
			resetIndex();
		}
	}

	public void close() {
		e.close();
		idx.close();
		te.close();
	}

	public Constant getVal(String fldname) {
		if (te.hasField(fldname))
			return te.getVal(fldname);
		else
			return e.getVal(fldname);
	}

	public int getInt(String fldname) {
		if (te.hasField(fldname))
			return te.getInt(fldname);
		else
			return e.getInt(fldname);
	}

	public long getLong(String fldname) {
		if (te.hasField(fldname))
			return te.getLong(fldname);
		else
			return e.getLong(fldname);
	}

	public float getFloat(String fldname) {
		if (te.hasField(fldname))
			return te.getFloat(fldname);
		else
			return e.getFloat(fldname);
	}

	public double getDouble(String fldname) {
		if (te.hasField(fldname))
			return te.getDouble(fldname);
		else
			return e.getDouble(fldname);
	}

	public String getString(String fldname) {
		if (te.hasField(fldname))
			return te.getString(fldname);
		else
			return e.getString(fldname);
	}

	public boolean hasField(String fldname) {
		return te.hasField(fldname) || e.hasField(fldname);
	}
	
	public boolean hasField(String fldname, String tblname) {
		return te.hasField(fldname, tblname);
	}

	private void resetIndex() {
		Constant searchkey = e.getVal(joinfield);
		idx.moveToHead(searchkey);
	}
}
