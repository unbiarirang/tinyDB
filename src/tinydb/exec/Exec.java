package tinydb.exec;

public interface Exec {
	public void beforeFirst();

	public boolean next();

	public void close();

	public Constant getVal(String fldname);

	public int getInt(String fldname);

	public long getLong(String fldname);

	public float getFloat(String fldname);

	public double getDouble(String fldname);

	public String getString(String fldname);

	public boolean hasField(String fldname);
}
