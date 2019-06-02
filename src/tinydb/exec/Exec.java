package tinydb.exec;

import tinydb.exec.consts.Constant;

// A query execution for each query operation
// Deal with records through the RecordManager
// Most methods just delegate to the RecordManager methods
public interface Exec {
	public void moveToHead(); // Move to head of record
	public boolean next();	  // Move to next record
	public void close();	  // Close the record
	public boolean hasField(String fldname); // If its schema has the specific field
	public boolean hasField(String fldname, String tblname);

	public Constant getVal(String fldname);
	public String getValToString(String fldname);
	public int getInt(String fldname);
	public long getLong(String fldname);
	public float getFloat(String fldname);
	public double getDouble(String fldname);
	public String getString(String fldname);
}
