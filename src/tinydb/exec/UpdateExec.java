package tinydb.exec;

import tinydb.exec.consts.Constant;
import tinydb.record.RID;

// A update execution for each query operation
// Update records through the RecordManager
// Most methods just delegate to the RecordManager methods
public interface UpdateExec extends Exec {
	public void setVal(String fldname, Constant val);
	public void setInt(String fldname, int val);
	public void setLong(String fldname, long val);
	public void setFloat(String fldname, float val);
	public void setDouble(String fldname, double val);
	public void setString(String fldname, String val);

	public void insert();
	public void delete();
	public RID getRid();
	public void moveToRid(RID rid);
}
