package tinydb.exec;

import java.util.ArrayList;
import java.util.Iterator;

import tinydb.exec.consts.Constant;

public class ProductExec implements Exec {
	private Exec e1, e2;

	public ProductExec(Exec e1, Exec e2) {
		this.e1 = e1;
		this.e2 = e2;
		e1.next();
	}

	// Exec methods //

	// LHS exec is positioned at its first record
	// RHS exec is positioned at its head
	public void moveToHead() {
		e1.moveToHead();
		e1.next();
		e2.moveToHead();
	}

	public boolean next() {
		if (e2.next())
			return true;
		
		e2.moveToHead();
		return e2.next() && e1.next();
	}

	public void close() {
		e1.close();
		e2.close();
	}

	public Constant getVal(String fldname) {
		if (e1.hasField(fldname))
			return e1.getVal(fldname);
		else
			return e2.getVal(fldname);
	}
	
	public Constant getValWithTable(String fldname, String tblname) {
		if (e1.hasField(fldname, tblname))
			return e1.getVal(fldname);
		else
			return e2.getVal(fldname);
	}
	
	public String getAllVal() throws Exception {
		return e1.getAllVal() + e2.getAllVal();
	}
	
	public String getAllVal(ArrayList<String> fieldlist) throws Exception {
		return e1.getAllVal(fieldlist);
	}

	public String getAllVal(ArrayList<String> tablelist, ArrayList<String> fieldlist) throws Exception {
		Iterator<String> it1 = tablelist.iterator();
		Iterator<String> it2 = fieldlist.iterator();

		String res = "";
		while (it1.hasNext() && it2.hasNext()) {
		    String tblname = it1.next();
		    String fldname = it2.next();
		    if (!tblname.contentEquals("")) {
		    	if (e1.hasTable(tblname) && e1.hasField(fldname))
		    		res += e1.getValToString(fldname) + "\t";
		    	else
		    		res += e2.getValToString(fldname) + "\t";
		    } else {
		    	if (e1.hasField(fldname))
		    		res += e1.getValToString(fldname) + "\t";
		    	else
		    		res += e2.getValToString(fldname) + "\t";
		    }
		}
		return res;
	}
	
	public String getValToString(String fldname) {
		if (e1.hasField(fldname))
			return e1.getValToString(fldname);
		else
			return e2.getValToString(fldname);
	}

	public int getInt(String fldname) {
		if (e1.hasField(fldname))
			return e1.getInt(fldname);
		else
			return e2.getInt(fldname);
	}

	public long getLong(String fldname) {
		if (e1.hasField(fldname))
			return e1.getInt(fldname);
		else
			return e2.getInt(fldname);
	}

	public float getFloat(String fldname) {
		if (e1.hasField(fldname))
			return e1.getFloat(fldname);
		else
			return e2.getFloat(fldname);
	}

	public double getDouble(String fldname) {
		if (e1.hasField(fldname))
			return e1.getDouble(fldname);
		else
			return e2.getDouble(fldname);
	}

	public String getString(String fldname) {
		if (e1.hasField(fldname))
			return e1.getString(fldname);
		else
			return e2.getString(fldname);
	}

	public boolean hasField(String fldname) {
		return e1.hasField(fldname) || e2.hasField(fldname);
	}
	
	public boolean hasField(String fldname, String tblname) {
		return e1.hasField(fldname, tblname) || e2.hasField(fldname, tblname);
	}
	
	public boolean hasTable(String tblname) {
		return e1.hasTable(tblname) || e2.hasTable(tblname);
	}
}
