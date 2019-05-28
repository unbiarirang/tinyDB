package tinydb.plan;

import tinydb.exec.*;
import tinydb.record.Schema;
import java.util.Collection;

public class ProjectPlan implements Plan {
	private Plan p;
	private Schema schema = new Schema();

	public ProjectPlan(Plan p, Collection<String> fieldlist) {
		this.p = p;
		for (String fldname : fieldlist)
			schema.add(fldname, p.schema());
	}

	public Exec exec() {
		Exec e = p.exec();
		// return new ProjectExec(s, schema.fields());
		return new ProjectExec(e, ((SelectPlan) p).lhstables(), ((SelectPlan) p).rhsfields());
	}

	public int blocksAccessed() {
		return p.blocksAccessed();
	}

	public int recordsOutput() {
		return p.recordsOutput();
	}

	public int distinctValues(String fldname) {
		return p.distinctValues(fldname);
	}

	public Schema schema() {
		return schema;
	}
}
