package tinydb.exec;

import java.util.ArrayList;

import tinydb.exec.consts.Constant;
import tinydb.exec.expr.Condition;
import tinydb.record.*;

public class SelectExec implements UpdateExec {
	private Exec e;
	private Condition cond;

	public SelectExec(Exec e, Condition cond) {
		this.e = e;
		this.cond = cond;
	}

	// Exec methods

	public void moveToHead() {
		e.moveToHead();
	}

	public boolean next() {
		while (e.next()) {
			if ((!cond.isOr() && cond.isSatisfiedAnd(e)) || (cond.isOr() && cond.isSatisfiedOr(e)))
				return true;
		}
		return false;
	}

	public void close() {
		e.close();
	}

	public Constant getVal(String fldname) {
		return e.getVal(fldname);
	}
	
	public String getAllVal() throws Exception {
		return e.getAllVal();
	}
	
	public String getAllVal(ArrayList<String> fieldlist) throws Exception {
		return e.getAllVal(fieldlist);
	}
	
	public String getAllVal(ArrayList<String> tablelist, ArrayList<String> fieldlist) throws Exception {
		return e.getAllVal(tablelist, fieldlist);
	}

	public String getValToString(String fldname) {
		return e.getValToString(fldname);
	}

	public int getInt(String fldname) {
		return e.getInt(fldname);
	}

	public long getLong(String fldname) {
		return e.getLong(fldname);
	}

	public float getFloat(String fldname) {
		return e.getFloat(fldname);
	}

	public double getDouble(String fldname) {
		return e.getDouble(fldname);
	}

	public String getString(String fldname) {
		return e.getString(fldname);
	}

	public boolean hasField(String fldname) {
		return e.hasField(fldname);
	}
	
	public boolean hasField(String fldname, String tblname) {
		return e.hasField(fldname, tblname);
	}
	
	public boolean hasTable(String tblname) {
		return e.hasTable(tblname);
	}

	// UpdateExec methods

	public void setVal(String fldname, Constant val) {
		UpdateExec us = (UpdateExec) e;
		us.setVal(fldname, val);
	}

	public void setInt(String fldname, int val) {
		UpdateExec us = (UpdateExec) e;
		us.setInt(fldname, val);
	}

	public void setLong(String fldname, long val) {
		UpdateExec us = (UpdateExec) e;
		us.setLong(fldname, val);
	}

	public void setFloat(String fldname, float val) {
		UpdateExec us = (UpdateExec) e;
		us.setFloat(fldname, val);
	}

	public void setDouble(String fldname, double val) {
		UpdateExec us = (UpdateExec) e;
		us.setDouble(fldname, val);
	}

	public void setString(String fldname, String val) {
		UpdateExec us = (UpdateExec) e;
		us.setString(fldname, val);
	}

	public void delete() {
		UpdateExec us = (UpdateExec) e;
		us.delete();
	}

	public void insert() {
		UpdateExec us = (UpdateExec) e;
		us.insert();
	}

	public RID getRid() {
		UpdateExec us = (UpdateExec) e;
		return us.getRid();
	}

	public void moveToRid(RID rid) {
		UpdateExec us = (UpdateExec) e;
		us.moveToRid(rid);
	}
}
