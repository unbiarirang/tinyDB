package tinydb.metadata;

import tinydb.record.*;
import tinydb.index.Index;

public interface IndexInfo {
	public Index open();

	public int blocksAccessed();

	public int recordsOutput();

	public int distinctValues(String fname);

	public Schema schema();
}
