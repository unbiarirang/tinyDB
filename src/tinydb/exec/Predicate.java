package tinydb.exec;

import tinydb.plan.Plan;
import tinydb.record.Schema;
import java.util.*;

//Boolean combination of Compares.
public class Predicate {
	private List<Comparison> terms = new ArrayList<Comparison>();
	public boolean isOr = false;

	public Predicate() {}

	public Predicate(Comparison t) {
		terms.add(t);
	}

	public void conjoinWith(Predicate pred) {
		terms.addAll(pred.terms);
	}

	public boolean isSatisfied(Exec e) {
		for (Comparison t : terms)
			if (!t.isSatisfied(e))
				return false;
		return true;
	}

	public boolean isSatisfiedOr(Exec e) {
		for (Comparison t : terms)
			if (t.isSatisfied(e))
				return true;
		return false;
	}

	public int reductionFactor(Plan p) {
		int factor = 1;
		for (Comparison t : terms)
			factor *= t.reductionFactor(p);
		return factor;
	}
	
	public Predicate selectPred(Schema sch) {
		Predicate result = new Predicate();
		for (Comparison t : terms)
			if (t.appliesTo(sch))
				result.terms.add(t);
		if (result.terms.size() == 0)
			return null;
		else
			return result;
	}

	public Predicate joinPred(Schema sch1, Schema sch2) {
		Predicate result = new Predicate();
		Schema newsch = new Schema();
		newsch.addAll(sch1);
		newsch.addAll(sch2);
		for (Comparison t : terms)
			if (!t.appliesTo(sch1) && !t.appliesTo(sch2) && t.appliesTo(newsch))
				result.terms.add(t);
		if (result.terms.size() == 0)
			return null;
		else
			return result;
	}

	public Constant equatesWithConstant(String fldname) {
		for (Comparison t : terms) {
			Constant c = t.equatesWithConstant(fldname);
			if (c != null)
				return c;
		}
		return null;
	}

	public String equatesWithField(String fldname) {
		for (Comparison t : terms) {
			String s = t.equatesWithField(fldname);
			if (s != null)
				return s;
		}
		return null;
	}

	public String toString() {
		Iterator<Comparison> iter = terms.iterator();
		if (!iter.hasNext())
			return "";
		String result = iter.next().toString();
		while (iter.hasNext())
			result += " and " + iter.next().toString();
		return result;
	}
}
