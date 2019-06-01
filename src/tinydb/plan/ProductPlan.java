package tinydb.plan;

import tinydb.exec.*;
import tinydb.record.Schema;

// Plan for product operation
public class ProductPlan implements Plan {
	private Plan p1, p2;
	private Schema schema = new Schema();

	public ProductPlan(Plan p1, Plan p2) {
		this.p1 = p1;
		this.p2 = p2;
		schema.addAll(p1.schema());
		schema.addAll(p2.schema());
	}

	// Plan methods //
	public Exec exec() {
		Exec e1 = p1.exec();
		Exec e2 = p2.exec();
		return new ProductExec(e1, e2);
	}

	public int blocksAccessed() {
		// p1 is outer table and p2 is inner table
		return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
	}

	public int recordsOutput() {
		return p1.recordsOutput() * p2.recordsOutput();
	}

	public int distinctValues(String fldname) {
		if (p1.schema().hasField(fldname))
			return p1.distinctValues(fldname);
		else
			return p2.distinctValues(fldname);
	}

	public Schema schema() {
		return schema;
	}
}
