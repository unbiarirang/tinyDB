package tinydb.exec.expr;

import tinydb.exec.Exec;
import tinydb.exec.consts.Constant;
import tinydb.record.Schema;

public class ConstantExpression implements Expression {
	private Constant val;

	public ConstantExpression(Constant c) {
		this.val = c;
	}

	public boolean isConstant() {
		return true;
	}

	public boolean isFieldName() {
		return false;
	}

	public Constant getConst() {
		return this.val;
	}

	public String getFieldName() {
		throw new ClassCastException();
	}

	// Just return constant value regardless of execution
	public Constant evaluate(Exec e) {
		return this.val;
	}
	
	// Just return constant value regardless of execution
	public Constant evaluateWithTable(Exec e, String lhsTableName) {
		return this.val;
	}

	// Constant values can exist in any schema
	public boolean existIn(Schema sch) {
		return true;
	}
	
	public String getTableName() {
		return null;
	}
}
