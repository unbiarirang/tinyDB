package tinydb.exec;

import tinydb.record.Schema;

public class FieldNameExpression implements Expression {
	private String fldname;

	public FieldNameExpression(String fldname) {
		this.fldname = fldname;
	}

	public FieldNameExpression(String tblname, String fldname) {
		this.fldname = fldname;
	}

	public boolean isConstant() {
		return false;
	}

	public boolean isFieldName() {
		return true;
	}

	public Constant asConstant() {
		throw new ClassCastException();
	}

	public String asFieldName() {
		return fldname;
	}

	public Constant evaluate(Exec e) {
		return e.getVal(fldname);
	}

	public boolean appliesTo(Schema sch) {
		return sch.hasField(fldname);
	}

	public String toString() {
		return fldname;
	}
}
