package tinydb.exec.expr;

import tinydb.exec.Exec;
import tinydb.exec.consts.Constant;
import tinydb.plan.Plan;
import tinydb.record.Schema;

// Comparison between two expressions.
// e.g. id = 1 (fldname-const), i_name = s_name (fldname-fldname)
public class Comparison {
	private Expression lhs, rhs;
	private String relation;		// relationship between two expressions
									// contains "=", ">", ">=", "<", "<=", "<>"

	public Comparison(Expression lhs, Expression rhs, String r) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.relation = r;
	}

	// Extent that selecting on the term can reduce the number of output records
	public int reductionFactor(Plan p) {
		String lhsName, rhsName;
		if (lhs.isFieldName() && rhs.isFieldName()) {
			lhsName = lhs.getFieldName();
			rhsName = rhs.getFieldName();
			return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
		}
		if (lhs.isFieldName()) {
			lhsName = lhs.getFieldName();
			return p.distinctValues(lhsName);
		}
		if (rhs.isFieldName()) {
			rhsName = rhs.getFieldName();
			return p.distinctValues(rhsName);
		}
		// otherwise, the term equates constants
		if (lhs.getConst().equals(rhs.getConst()))
			return 1;
		else
			return Integer.MAX_VALUE;
	}

	// In case of Field=Const, given the fldname return the const value
	public Constant getFieldValue(String fldname) {
		if (lhs.isFieldName() && lhs.getFieldName().equals(fldname) && rhs.isConstant())
			return rhs.getConst();
		else if (rhs.isFieldName() && rhs.getFieldName().equals(fldname) && lhs.isConstant())
			return lhs.getConst();
		else
			return null;
	}

	// In case of Field1=Field2, given the fldname1 return the fldname2
	public String getAnotherFieldName(String fldname) {
		if (lhs.isFieldName() && lhs.getFieldName().equals(fldname) && rhs.isFieldName())
			return rhs.getFieldName();
		else if (rhs.isFieldName() && rhs.getFieldName().equals(fldname) && lhs.isFieldName())
			return lhs.getFieldName();
		else
			return null;
	}

	// If both of the expressions exist in the schema
	public boolean existIn(Schema sch) {
		return lhs.existIn(sch) && rhs.existIn(sch);
	}

	// If both of expressions satisfy the relationship between them
	public boolean isSatisfied(Exec e) {
		String tblname1 = getLhsTableName();
		String tblname2 = getRhsTableName();
		Constant lhsval, rhsval;

		// e.g. tblname1.fldname1 = tblname2.fldname2
		if (tblname1 != null && tblname2 != null && tblname1 != "" && tblname2 != null) {
				//&& !lhs.getFieldName().contentEquals(rhs.getFieldName())) {
			lhsval = lhs.evaluateWithTable(e, tblname1);
			rhsval = rhs.evaluateWithTable(e, tblname2);
		}
		// e.g. 1 = tblname.fldname
		else if (tblname1 != null && (tblname2 == null || tblname2 == "") && tblname1 != "") {
			lhsval = lhs.evaluateWithTable(e, tblname1);
			rhsval = rhs.evaluate(e);
		}
		// e.g. tblname.fldname = 1
		else if (tblname2 != null && (tblname1 == null || tblname1 == "") && tblname2 != "") {
			lhsval = lhs.evaluate(e);
			rhsval = rhs.evaluateWithTable(e, tblname2);
		}
		// e.g. fldname1 = fldname2 or fldname = 1
		else {
			lhsval = lhs.evaluate(e);
			rhsval = rhs.evaluate(e);
		}
		
		if (lhsval == null) lhsval = lhs.evaluate(e);
		if (rhsval == null) rhsval = rhs.evaluate(e);
		
		if (lhsval.isNull() || rhsval.isNull()) return false;
		
		switch (relation) {
		case ">":
			return lhsval.compareTo(rhsval) > 0;
		case ">=":
			return lhsval.compareTo(rhsval) >= 0;
		case "<":
			return lhsval.compareTo(rhsval) < 0;
		case "<=":
			return lhsval.compareTo(rhsval) <= 0;
		case "<>":
			return lhsval.compareTo(rhsval) != 0;
		}

		// case of "="
		return lhsval.equals(rhsval);
	}
	
	public void modifyRhsValue(Expression rhs) {
		this.rhs = rhs;
	}
	
	public String getLhsTableName() {
		return lhs.getTableName();
	}
	
	public String getRhsTableName() {
		return rhs.getTableName();
	}
	
	public boolean isLhsFieldName() {
		return lhs.isFieldName();
	}
	
	public boolean isRhsFieldName() {
		return rhs.isFieldName();
	}
	
	public String getLhsFieldName() {
		return lhs.getFieldName();
	}
	
	public String getRhsFieldName() {
		return rhs.getFieldName();
	}
	
	public String getRelation() {
		return relation;
	}
	
	public Constant getRhsValue() {
		return rhs.getConst();
	}
}
