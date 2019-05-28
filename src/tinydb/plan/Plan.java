package tinydb.plan;

import tinydb.record.Schema;
import tinydb.exec.Exec;

// A query plan for each query operation
public interface Plan {

	public Exec exec();

	public int blocksAccessed();

	public int recordsOutput();

	public int distinctValues(String fldname);

	public Schema schema();
}
