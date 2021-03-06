package tinydb.plan;

import tinydb.record.Schema;
import tinydb.metadata.IndexInfo;
import tinydb.exec.*;
import tinydb.exec.consts.Constant;
import tinydb.index.Index;

// Plan for indexselect operation
public class IndexSelectPlan implements Plan {
	private TablePlan p;
	private IndexInfo ii;
	private Constant val;
	private String relation;
	private boolean isOr;

	public IndexSelectPlan(TablePlan p, IndexInfo ii, Constant val, String relation, boolean isOr) {
		this.p = p;
		this.ii = ii;
		this.val = val;
		this.relation = relation;
		this.isOr = isOr;
	}

	// Plan methods //
	public Exec exec() {
		// throws an exception if p is not a tableplan
		TableExec te = (TableExec) p.exec();
		Index idx = ii.open();
		return new IndexSelectExec(idx, val, te, relation, isOr);
	}
	
	// Index traversal cost plus the number of matching data records
	// B(indexselect(p,idx)) = B(idx) + R(indexselect(p,idx)) 
	public int blocksAccessed() {
		return ii.blocksAccessed() + recordsOutput();
	}

	public int recordsOutput() {
		return ii.recordsOutput();
	}

	public int distinctValues(String fldname) {
		return ii.distinctValues(fldname);
	}

	public Schema schema() {
		return p.schema();
	}
}
