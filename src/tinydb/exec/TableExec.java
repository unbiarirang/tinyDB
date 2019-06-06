package tinydb.exec;

import static tinydb.consts.Types.*;

import java.util.ArrayList;

import tinydb.exec.consts.Constant;
import tinydb.exec.consts.DoubleConstant;
import tinydb.exec.consts.FloatConstant;
import tinydb.exec.consts.IntConstant;
import tinydb.exec.consts.LongConstant;
import tinydb.exec.consts.StringConstant;
import tinydb.record.*;
import tinydb.util.BadSyntaxException;
import tinydb.util.DuplicatedException;

// Just a wrapper for a RecordManager
// The most basic Exec, execution for a corresponding table
public class TableExec implements UpdateExec {
	private RecordManager rm;
	private Schema sch;

	public TableExec(Table tb) {
		rm = new RecordManager(tb);
		sch = tb.schema();
	}
	
	// Exec methods //
	public void moveToHead() {
		rm.moveToHead();
	}
	
	public void moveTo(int n) {
		rm.moveTo(n);
	}

	public boolean next() {
		return rm.next();
	}

	public void close() {
		rm.close();
	}

	public Constant getVal(String fldname) {
		if (sch.type(fldname) == INTEGER)
			return new IntConstant(rm.getInt(fldname));
		else if (sch.type(fldname) == LONG)
			return new LongConstant(rm.getLong(fldname));
		else if (sch.type(fldname) == FLOAT)
			return new FloatConstant(rm.getFloat(fldname));
		else if (sch.type(fldname) == DOUBLE)
			return new DoubleConstant(rm.getDouble(fldname));
		else
			return new StringConstant(rm.getString(fldname));
	}
	
	public String getAllVal() {
		String res = "";
		for (String fldname : sch.fields())
			res += getValToString(fldname) + " ";
		
		return res;
	}
	
	public String getAllVal(ArrayList<String> fieldlist) {
		String res = "";
		for (String fldname : fieldlist)
			res += getValToString(fldname) + " ";
		
		return res;
	}

	// Error
	public String getAllVal(ArrayList<String> tablelist, ArrayList<String> fieldlist) throws Exception {
		throw new Exception("Not implemented!");
	}
	
	public String getValToString(String fldname) {
		if (rm.isNull(fldname)) // null
			return "null";
		
		if (sch.type(fldname) == INTEGER)
			return new IntConstant(rm.getInt(fldname)).toString();
		else if (sch.type(fldname) == LONG)
			return new LongConstant(rm.getLong(fldname)).toString();
		else if (sch.type(fldname) == FLOAT)
			return new FloatConstant(rm.getFloat(fldname)).toString();
		else if (sch.type(fldname) == DOUBLE)
			return new DoubleConstant(rm.getDouble(fldname)).toString();
		else
			return new StringConstant(rm.getString(fldname)).toString();
	}

	public int getInt(String fldname) {
		return rm.getInt(fldname);
	}

	public long getLong(String fldname) {
		return rm.getLong(fldname);
	}

	public float getFloat(String fldname) {
		return rm.getFloat(fldname);
	}

	public double getDouble(String fldname) {
		return rm.getDouble(fldname);
	}

	public String getString(String fldname) {
		return rm.getString(fldname);
	}

	public boolean hasField(String fldname) {
		return sch.hasField(fldname);
	}
	
	public boolean hasField(String fldname, String tblname) {
		return sch.hasField(fldname) && hasTable(tblname);
	}
	
	public boolean hasTable(String tblname) {
		return getTableName().contentEquals(tblname);
	}

	// UpdateExec methods //
	public void setVal(String fldname, Constant val) {
		if (val == null) {
			rm.setNull(fldname);
			return;
		}

		// Check if primary key value is duplicated
		if (sch.getPk().contentEquals(fldname) && rm.isValExist(fldname, val.value()))
			throw new DuplicatedException("The primary key value is duplicated");

		try {
			if (sch.type(fldname) == INTEGER)
				rm.setInt(fldname, (Integer) ((Long) val.value()).intValue());
			else if (sch.type(fldname) == LONG)
				rm.setLong(fldname, (Long) val.value());
			else if (sch.type(fldname) == FLOAT) {
				try {
					rm.setFloat(fldname, (Float) ((Double) val.value()).floatValue());
				} catch (java.lang.ClassCastException e) {
					rm.setFloat(fldname, (Float) ((Long) val.value()).floatValue());
				}
			} else if (sch.type(fldname) == DOUBLE)
				try {
					rm.setDouble(fldname, (Double) val.value());
				} catch (java.lang.ClassCastException e) {
					rm.setDouble(fldname, (Double) ((Long) val.value()).doubleValue());
				}
			else
				rm.setString(fldname, (String) val.value());
		} catch (ClassCastException e) {
			throw new BadSyntaxException("Attributs and value types do not match");
		}
	}

	public void setInt(String fldname, int val) {
		rm.setInt(fldname, val);
	}

	public void setLong(String fldname, long val) {
		rm.setLong(fldname, val);
	}
	
	public void setFloat(String fldname, float val) {
		rm.setFloat(fldname, val);
	}
	
	public void setDouble(String fldname, double val) {
		rm.setDouble(fldname, val);
	}

	public void setString(String fldname, String val) {
		rm.setString(fldname, val);
	}

	public void delete() {
		rm.delete();
	}

	public void insert() {
		rm.insert();
	}

	public RID getRid() {
		return rm.currentRid();
	}

	public void moveToRid(RID rid) {
		rm.moveToRid(rid);
	}
	
	public String getTableName() {
		return rm.getTableName();
	}
}
