package tinydb.plan;

import tinydb.record.Schema;
import tinydb.metadata.IndexInfo;
import tinydb.exec.*;
import tinydb.index.Index;

public class IndexJoinPlan implements Plan {
	private Plan p1;
	private TablePlan p2;
	private IndexInfo ii;
	private String joinfield;
	private Schema sch = new Schema();

	public IndexJoinPlan(Plan p1, TablePlan p2, IndexInfo ii, String joinfield) {
		this.p1 = p1;
		this.p2 = p2;
		this.ii = ii;
		this.joinfield = joinfield;
		sch.addAll(p1.schema());
		sch.addAll(p2.schema());
	}

	// Plan methods //
	public Exec exec() {
		Exec e = p1.exec();
		// throws an exception if p2 is not a tableplan
		TableExec te = (TableExec) p2.exec();
		Index idx = ii.open();
		return new IndexJoinExec(e, te, idx, joinfield);
	}

	// B(indexjoin(p1,p2,idx)) = B(p1) + R(p1) * B(idx)
	// 						   + R(indexjoin(p1,p2,idx))
	public int blocksAccessed() {
		return p1.blocksAccessed() + (p1.recordsOutput() * ii.blocksAccessed())
				+ recordsOutput();
	}

	// R(indexjoin(p1,p2,idx)) = R(p1) * R(idx)
	public int recordsOutput() {
		return p1.recordsOutput() * ii.recordsOutput();
	}

	public int distinctValues(String fldname) {
		if (p1.schema().hasField(fldname))
			return p1.distinctValues(fldname);
		else
			return p2.distinctValues(fldname);
	}

	public Schema schema() {
		return sch;
	}
}
