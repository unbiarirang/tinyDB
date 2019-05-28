package tinydb.plan;

import java.util.Collection;

import tinydb.exec.*;
import tinydb.record.Schema;

public class SelectPlan implements Plan {
	private Plan p;
	private Predicate pred;
	private Collection<String> lhstables;
	private Collection<String> rhsfields;

	public SelectPlan(Plan p, Predicate pred) {
		this.p = p;
		this.pred = pred;
	}

	public SelectPlan(Plan p, Predicate pred, Collection<String> lhstables, Collection<String> rhsfields) {
		this.p = p;
		this.pred = pred;
		this.lhstables = lhstables;
		this.rhsfields = rhsfields;
	}

	public Collection<String> lhstables() {
		return lhstables;
	}

	public Collection<String> rhsfields() {
		return rhsfields;
	}

	public Exec exec() {
		Exec e = p.exec();
		return new SelectExec(e, pred);
	}

	public int blocksAccessed() {
		return p.blocksAccessed();
	}

	public int recordsOutput() {
		return p.recordsOutput() / pred.reductionFactor(p);
	}

	public int distinctValues(String fldname) {
		if (pred.equatesWithConstant(fldname) != null)
			return 1;
		else {
			String fldname2 = pred.equatesWithField(fldname);
			if (fldname2 != null)
				return Math.min(p.distinctValues(fldname), p.distinctValues(fldname2));
			else
				return Math.min(p.distinctValues(fldname), recordsOutput());
		}
	}

	public Schema schema() {
		return p.schema();
	}
}
