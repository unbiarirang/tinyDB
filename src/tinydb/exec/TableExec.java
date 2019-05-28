package tinydb.exec;

import static tinydb.consts.Types.*;
import tinydb.parse.BadSyntaxException;
import tinydb.record.*;

public class TableExec implements UpdateExec {
	private RecordManager rm;
	private Schema sch;

	public TableExec(Table ti) {
		rm = new RecordManager(ti);
		sch = ti.schema();
	}

	// Exec methods

	public void beforeFirst() {
		rm.moveToFirst();
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

	// UpdateExec methods
	public void setVal(String fldname, Constant val) {
		if (val == null)
			return;

		try {
			if (sch.type(fldname) == INTEGER)
				rm.setInt(fldname, (Integer) ((Long) val.asJavaVal()).intValue());
			else if (sch.type(fldname) == LONG)
				rm.setLong(fldname, (Long) val.asJavaVal());
			else if (sch.type(fldname) == FLOAT) {
				try {
					rm.setFloat(fldname, (Float) ((Double) val.asJavaVal()).floatValue());
				} catch (java.lang.ClassCastException e) {
					rm.setFloat(fldname, (Float) ((Long) val.asJavaVal()).floatValue());
				}
			} else if (sch.type(fldname) == DOUBLE)
				try {
					rm.setDouble(fldname, (Double) val.asJavaVal());
				} catch (java.lang.ClassCastException e) {
					rm.setDouble(fldname, (Double) ((Long) val.asJavaVal()).doubleValue());
				}
			else
				rm.setString(fldname, (String) val.asJavaVal());
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
}
