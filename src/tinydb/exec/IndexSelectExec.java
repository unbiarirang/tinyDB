package tinydb.exec;

import tinydb.record.RID;

import java.util.ArrayList;

import tinydb.exec.consts.Constant;
import tinydb.index.Index;

public class IndexSelectExec implements Exec {
	private Index idx;
	private Constant val;  // index search key
	private TableExec te;

	public IndexSelectExec(Index idx, Constant val, TableExec te) {
		this.idx = idx;
		this.val = val;
		this.te = te;
		moveToHead();
	}
	
	// Exec methods //
	public void moveToHead() {
		idx.moveToHead(val);
	}

	public boolean next() {
		boolean ok = idx.next();
		if (ok) {
			RID rid = idx.getDataRid();
			te.moveToRid(rid);
		}
		return ok;
	}

	public void close() {
		idx.close();
		te.close();
	}

	public Constant getVal(String fldname) {
		return te.getVal(fldname);
	}
	
	public String getAllVal() {
		return te.getAllVal();
	}
	
	public String getAllVal(ArrayList<String> fieldlist) {
		return te.getAllVal(fieldlist);
	}

	public String getAllVal(ArrayList<String> tablelist, ArrayList<String> fieldlist) throws Exception {
		throw new Exception("Not implemented!");
	}
	
	public String getValToString(String fldname) {
		return te.getValToString(fldname);
	}

	public int getInt(String fldname) {
		return te.getInt(fldname);
	}

	public long getLong(String fldname) {
		return te.getLong(fldname);
	}

	public float getFloat(String fldname) {
		return te.getFloat(fldname);
	}

	public double getDouble(String fldname) {
		return te.getDouble(fldname);
	}

	public String getString(String fldname) {
		return te.getString(fldname);
	}

	public boolean hasField(String fldname) {
		return te.hasField(fldname);
	}
	
	public boolean hasField(String fldname, String tblname) {
		return te.hasField(fldname, tblname);
	}
	
	public boolean hasTable(String tblname) {
		return te.hasTable(tblname);
	}
}
