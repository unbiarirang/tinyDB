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
		if (((SelectPlan) p).lhstables() != null && ((SelectPlan) p).rhsfields() != null)
			return new ProjectExec(e, ((SelectPlan) p).lhstables(), ((SelectPlan) p).rhsfields());
		
		return new ProjectExec(e, schema.fields());
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
