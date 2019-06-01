package tinydb.exec.expr;

import java.util.*;

import tinydb.exec.Exec;
import tinydb.exec.consts.Constant;
import tinydb.plan.Plan;
import tinydb.record.Schema;
import tinydb.util.Tuple;

// Query condition, boolean combination of Comparisons.
// Support only all of 'and' combination and all of 'or' combination
public class Condition {
	private ArrayList<Comparison> terms = new ArrayList<Comparison>();
	public boolean isOr = false;

	public Condition() {
	}

	public Condition(Comparison t) {
		terms.add(t);
	}

	// Join two conditions
	public void join(Condition cond) {
		terms.addAll(cond.terms);
	}

	// Evaluate 'and' combination of all comparisons
	public boolean isSatisfiedAnd(Exec e) {
		for (Comparison t : terms)
			if (!t.isSatisfied(e))
				return false;
		return true;
	}

	// Evaluate 'or' combination of all comparisons
	public boolean isSatisfiedOr(Exec e) {
		for (Comparison t : terms)
			if (t.isSatisfied(e))
				return true;
		return false;
	}

	// Extent that selecting on the term can reduce the number of output records
	public int reductionFactor(Plan p) {
		int factor = 1;
		for (Comparison t : terms)
			factor *= t.reductionFactor(p);
		return factor;
	}

	// subcondition that has all comparisons of specified schema
	public Condition selectCond(Schema sch) {
		Condition result = new Condition();
		for (Comparison t : terms) {
			// case1. a.id = b.id Ignore the condition
			if (t.isLhsFieldName() && t.isRhsFieldName()
					&& (t.getLhsFieldName().contentEquals(t.getRhsFieldName())))
				continue;
			
			// case2. id1 = id2
			if (t.existIn(sch))
				result.terms.add(t);
		}

		if (result.terms.size() == 0)
			return null;
		else
			return result;
	}

	// subcondition whose comparisons apply to the union of the two schemas but not
	// either schema separately.
	public Condition joinCond(Schema sch1, Schema sch2) {
		Condition result = new Condition();
		Schema newsch = new Schema();
		newsch.addAll(sch1);
		newsch.addAll(sch2);

		for (Comparison t : terms) {
			// e.g. id1 = id2
			if (!t.existIn(sch1) && !t.existIn(sch2) && t.existIn(newsch))
				result.terms.add(t);

			// e.g. a.id = b.id
			if (t.isLhsFieldName() && t.isRhsFieldName() &&
					t.getLhsFieldName().contentEquals(t.getRhsFieldName()))
				result.terms.add(t);
		}
		if (result.terms.size() == 0)
			return null;
		else
			return result;
	}

	public Constant getFieldValue(String fldname) {
		for (Comparison t : terms) {
			Constant c = t.getFieldValue(fldname);
			if (c != null)
				return c;
		}
		return null;
	}

	public String getAnotherFieldName(String fldname) {
		for (Comparison t : terms) {
			String s = t.getAnotherFieldName(fldname);
			if (s != null)
				return s;
		}
		return null;
	}

	public Tuple<Collection<String>, Collection<String>> appendFields(Collection<String> tableL,
			Collection<String> fieldL) {
		Collection<String> lhstables = new ArrayList<String>(tableL);
		Collection<String> rhsfields = new ArrayList<String>(fieldL);
		String tblname1, tblname2, fldname1, fldname2;
		for (Comparison t : terms) {
			tblname1 = t.getLhsTableName();
			tblname2 = t.getLhsTableName();
			if (tblname1 != null) {
				lhstables.add(tblname1);
				fldname1 = t.getLhsFieldName();
				rhsfields.add(fldname1);
			}
			if (tblname2 != null) {
				lhstables.add(tblname2);
				fldname2 = t.getLhsFieldName();
				rhsfields.add(fldname2);
			}
		}

		return new Tuple(lhstables, rhsfields);
	}
}
