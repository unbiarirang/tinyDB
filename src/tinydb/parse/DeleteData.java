package tinydb.parse;

import tinydb.exec.expr.Condition;

public class DeleteData {
	private String tblname;
	private Condition cond;

	public DeleteData(String tblname, Condition cond) {
		this.tblname = tblname;
		this.cond = cond;
	}

	public String tableName() {
		return tblname;
	}

	public Condition cond() {
		return cond;
	}
}
