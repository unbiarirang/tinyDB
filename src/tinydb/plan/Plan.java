package tinydb.plan;

import tinydb.record.Schema;
import tinydb.exec.Exec;

// A query plan for each query operation
public interface Plan {
	// Create a execution for the query.
	public Exec exec();

	// Estimation of the number of block to be accessed for the query
	public int blocksAccessed();

	// Estimation of the number of output records
	public int recordsOutput();

	// Estimation of the number of distinct values of the specified field
	// in the query's output table
	public int distinctValues(String fldname);

	// Just return relevant schema
	public Schema schema();
}