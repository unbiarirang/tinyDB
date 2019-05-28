package tinydb.exec;

import tinydb.record.Schema;

public class ConstantExpression implements Expression {
	private Constant val;

	public ConstantExpression(Constant c) {
		val = c;
	}

	public boolean isConstant() {
		return true;
	}

	public boolean isFieldName() {
		return false;
	}

	public Constant asConstant() {
		return val;
	}

	public String asFieldName() {
		throw new ClassCastException();
	}

	public Constant evaluate(Exec e) {
		return val;
	}

	public boolean appliesTo(Schema sch) {
		return true;
	}

	public String toString() {
		return val.toString();
	}
}
