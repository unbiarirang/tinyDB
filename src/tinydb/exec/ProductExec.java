package tinydb.exec;

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
}
