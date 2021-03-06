package tinydb.exec;

import java.util.*;

import tinydb.exec.consts.Constant;
import tinydb.util.Tuple;
import tinydb.util.Utils;

// Plan for project operation
public class ProjectExec implements Exec {
	private Exec e;
	private ArrayList<String> tablelist;
	private ArrayList<String> fieldlist;
	private ArrayList<String> resfieldlist; // fields to return as results

	public ProjectExec(Exec e, List<String> fieldlist) {
		this.e = e;
		this.fieldlist = (ArrayList<String>) fieldlist;
		this.resfieldlist = this.fieldlist;
	}

	public ProjectExec(Exec e, List<String> tablelist, List<String> fieldlist,
			List<String> resfieldlist) {
		this.e = e;
		this.tablelist = (ArrayList<String>) tablelist;
		this.fieldlist = (ArrayList<String>) fieldlist;
		this.resfieldlist = (ArrayList<String>) resfieldlist;
	}
	
	public String tables() {
		if (tablelist != null && !Utils.isAllEmpty(tablelist))
			return tablelist.subList(0, resfieldlist.size()).toString();
		
		return null;
	}
	
	public String fields() {
		return resfieldlist.toString();
	}

	// Exec methods //
	public void moveToHead() {
		e.moveToHead();
	}

	public boolean next() {
		close();
		return e.next();
	}

	public void close() {
		e.close();
	}

	public Constant getVal(String fldname) {
		Tuple<String, String> attr = splitTableField(fldname);
		String tblname = attr.x;
		fldname = attr.y;

		if (hasField(tblname, fldname))
			return e.getVal(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}
	
	public String getAllVal() throws Exception {
		try {
			if (tablelist != null && !Utils.isAllEmpty(tablelist))
				return e.getAllVal(tablelist, resfieldlist);
			try {
				return e.getAllVal(resfieldlist);
			} catch (Exception exx) {
				return e.getAllVal();
			}
		} catch (Exception ex) {
			try {
				return e.getAllVal(resfieldlist);
			} catch (Exception exx) {
				return e.getAllVal();
			}
		}
	}
	
	// Error
	public String getAllVal(ArrayList<String> fieldlist) throws Exception {
		throw new Exception("Not implemented!");
	}
	
	// Error
	public String getAllVal(ArrayList<String> tablelist, ArrayList<String> fieldlist) throws Exception {
		throw new Exception("Not implemented!");
	}

	public String getValToString(String fldname) {
		Tuple<String, String> attr = splitTableField(fldname);
		String tblname = attr.x;
		fldname = attr.y;

		if (hasField(tblname, fldname))
			return e.getValToString(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}

	public int getInt(String fldname) {
		Tuple<String, String> attr = splitTableField(fldname);
		String tblname = attr.x;
		fldname = attr.y;

		if (hasField(tblname, fldname))
			return e.getInt(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}

	public long getLong(String fldname) {
		Tuple<String, String> attr = splitTableField(fldname);
		String tblname = attr.x;
		fldname = attr.y;

		if (hasField(tblname, fldname))
			return e.getLong(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}

	public float getFloat(String fldname) {
		Tuple<String, String> attr = splitTableField(fldname);
		String tblname = attr.x;
		fldname = attr.y;

		if (hasField(tblname, fldname))
			return e.getFloat(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}

	public double getDouble(String fldname) {
		Tuple<String, String> attr = splitTableField(fldname);
		String tblname = attr.x;
		fldname = attr.y;

		if (hasField(tblname, fldname))
			return e.getDouble(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}

	public String getString(String fldname) {
		Tuple<String, String> attr = splitTableField(fldname);
		String tblname = attr.x;
		fldname = attr.y;

		if (hasField(tblname, fldname))
			return e.getString(fldname);
		else
			throw new RuntimeException("field " + fldname + " not found.");
	}

	public boolean hasField(String fldname) {
		return fieldlist.contains(fldname);
	}

	public boolean hasField(String fldname, String tblname) {
		if (tblname == "")
			return fieldlist.contains(fldname);
		else
			return fieldlist.contains(fldname) && tablelist.get(fieldlist.indexOf(fldname)).contentEquals(tblname);
	}
	
	public boolean hasTable(String tblname) {
		return tablelist.contains(tblname);
	}

	private Tuple<String, String> splitTableField(String fldname) {
		String[] temp = fldname.split("\\.");
		String tblname = "";
		if (temp.length == 2) {
			tblname = temp[0];
			fldname = temp[1];
			return new Tuple<String, String>(tblname, fldname);
		}
		return new Tuple<String, String>("", fldname);
	}
}
