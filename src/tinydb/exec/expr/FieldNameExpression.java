package tinydb.exec.expr;

import tinydb.exec.Exec;
import tinydb.exec.consts.Constant;
import tinydb.exec.ProductExec;
import tinydb.record.Schema;

public class FieldNameExpression implements Expression {
	private String fldname;
	private String tblname = null;

	public FieldNameExpression(String fldname) {
		this.fldname = fldname;
	}

	public FieldNameExpression(String fldname, String tblname) {
		this.fldname = fldname;
		this.tblname = tblname;
	}

	public boolean isConstant() {
		return false;
	}

	public boolean isFieldName() {
		return true;
	}

	public Constant getConst() {
		throw new ClassCastException();
	}

	public String getFieldName() {
		return fldname;
	}

	public Constant evaluate(Exec e) {
		return e.getVal(fldname);
	}
	
	public Constant evaluateWithTable(Exec e, String tblname) {
		if (e.hasField(fldname, tblname)) {
			try {
				return ((ProductExec)e).getValWithTable(fldname, tblname);
			} catch (ClassCastException ex) {
				return null;
			}
		}
		
		return null;
	}

	public boolean existIn(Schema sch) {
		return sch.hasField(fldname);
	}

	public String toString() {
		return fldname;
	}
	
	public String getTableName() {
		return tblname;
	}
}
