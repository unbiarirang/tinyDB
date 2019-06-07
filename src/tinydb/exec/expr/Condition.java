package tinydb.exec.expr;

import java.util.*;

import tinydb.exec.Exec;
import tinydb.exec.IndexSelectExec;
import tinydb.exec.consts.Constant;
import tinydb.plan.Plan;
import tinydb.record.Schema;
import tinydb.record.Table;
import tinydb.server.DBManager;
import tinydb.util.Tuple;
import tinydb.util.Utils;

// Query condition, boolean combination of Comparisons.
// Support only all of 'and' combination and all of 'or' combination
public class Condition {
	private ArrayList<Comparison> terms = new ArrayList<Comparison>();
	private boolean isOr = false;

	public Condition() {
	}
	
	public Condition(Comparison t) {
		terms.add(t);
	}

	public ArrayList<Comparison> terms() {
		return terms;
	}
	
	public boolean isOr() {
		return isOr;
	}

	public void setIsOR(boolean flag) {
		isOr = flag;
	}

	// Join two conditions
	public void join(Condition cond) {
		terms.addAll(cond.terms);
		if (cond.isOr())
			this.isOr = true;
	}
	
	public void remove(String fldname) {
		for (Comparison t : terms)
			if (t.getLhsFieldName().contentEquals(fldname))
				terms.remove(t);
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
		for (Comparison t : terms) {
			if (t.isSatisfied(e)) 
				return true;
			
//			if (e instanceof IndexSelectExec) {
//				((IndexSelectExec) e).setSearchKey(t.getRhsValue());
//			}
		}
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
		result.setIsOR(this.isOr);
		for (Comparison t : terms) {
			// case1. a.id = b.id Ignore the condition
			if (t.isLhsFieldName() && t.isRhsFieldName() && (t.getLhsFieldName().contentEquals(t.getRhsFieldName())))
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
		result.setIsOR(this.isOr);
		Schema newsch = new Schema();
		newsch.addAll(sch1);
		newsch.addAll(sch2);

		for (Comparison t : terms) {
			// e.g. id1 = id2
			if (!t.existIn(sch1) && !t.existIn(sch2) && t.existIn(newsch))
				result.terms.add(t);

			// e.g. a.id = b.id
			if (t.isLhsFieldName() && t.isRhsFieldName() && t.getLhsFieldName().contentEquals(t.getRhsFieldName()))
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
	
	public String getRelation(String fldname) {
		for (Comparison t : terms) {
			if (t.getLhsFieldName().contentEquals(fldname))
				return t.getRelation();
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

	public Tuple<List<String>, List<String>> appendFields(List<String> tableL,
			List<String> fieldL) {
		List<String> lhstables = tableL != null ? new ArrayList<String>(tableL) : new ArrayList<String>();
		List<String> rhsfields = fieldL != null ? new ArrayList<String>(fieldL) : new ArrayList<String>();
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

		return new Tuple<List<String>, List<String>>(lhstables, rhsfields);
	}

	// Support natural join between two relations
	public void addNaturalJoin(List<String> tables) {
		List<String> intersect = null;
		List<String> tblnames = new ArrayList<String>(tables);
		Table tb1 = DBManager.tableManager().getTable(tblnames.get(0));
		Table tb2 = DBManager.tableManager().getTable(tblnames.get(1));
		ArrayList<String> fldnames1 = tb1.fldnames();
		ArrayList<String> fldnames2 = tb2.fldnames();

		intersect = Utils.intersection(fldnames1, fldnames2);
		
		Condition cond = new Condition();
		for (String fldname : intersect) {
			Comparison c = new Comparison(new FieldNameExpression(fldname, tblnames.get(0))
										  , new FieldNameExpression(fldname, tblnames.get(1))
										  , "=");
			cond.join(new Condition(c));
		}
		
		this.join(cond);
	}
}
