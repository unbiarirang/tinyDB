package tinydb.plan;

import java.util.List;

import tinydb.exec.*;
import tinydb.exec.expr.Condition;
import tinydb.record.Schema;
import tinydb.util.Tuple;

// Plan for select operation
public class SelectPlan implements Plan {
	private Plan p;
	private Condition cond;
	private List<String> lhstables;
	private List<String> rhsfields;

	public SelectPlan(Plan p, Condition cond) {
		this.p = p;
		this.cond = cond;
	}

	public SelectPlan(Plan p, Condition cond, List<String> lhstables, List<String> rhsfields) {
		this.p = p;
		this.cond = cond;
		Tuple<List<String>, List<String>> fields = cond.appendFields(lhstables, rhsfields);
		this.lhstables = fields.x;
		this.rhsfields = fields.y;
	}

	public List<String> lhstables() {
		return lhstables;
	}

	public List<String> rhsfields() {
		return rhsfields;
	}

	// Plan methods //
	public Exec exec() {
		Exec e = p.exec();
		return new SelectExec(e, cond);
	}

	public int blocksAccessed() {
		return p.blocksAccessed();
	}

	// Consider the reduction factor of condition
	public int recordsOutput() {
		return p.recordsOutput() / cond.reductionFactor(p);
	}

	public int distinctValues(String fldname) {
		if (cond.getFieldValue(fldname) != null)
			return 1;

		String fldname2 = cond.getAnotherFieldName(fldname);
		if (fldname2 != null)
			return Math.min(p.distinctValues(fldname), p.distinctValues(fldname2));

		return Math.min(p.distinctValues(fldname), recordsOutput());
	}

	public Schema schema() {
		return p.schema();
	}
}
