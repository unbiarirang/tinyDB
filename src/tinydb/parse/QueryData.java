package tinydb.parse;

import tinydb.exec.expr.Condition;

import java.util.*;

// Data for the SQL SELECT statement.
public class QueryData {
	private List<String> lhstables;
	private List<String> fields;
	private List<String> tables;
	private Condition cond;
	private boolean isAll = false; // whether is applies to all fields

	public QueryData(List<String> lhstables, List<String> fields, List<String> tables,
			Condition cond) {
		this.lhstables = lhstables;
		this.fields = fields;
		this.tables = tables;
		this.cond = cond;
	}
	
	public QueryData(List<String> lhstables, List<String> fields, List<String> tables,
			Condition cond, boolean isNaturalJoin, boolean isAll) {
		this.lhstables = lhstables;
		this.fields = fields;
		this.tables = tables;
		this.cond = cond;
		this.isAll = isAll;
		if (isNaturalJoin)
			this.cond.addNaturalJoin(tables);
	}

	public List<String> lhstables() {
		return lhstables;
	}

	public List<String> fields() {
		return fields;
	}

	public List<String> tables() {
		return tables;
	}

	public Condition cond() {
		return cond;
	}
	
	public boolean isAll() {
		return isAll;
	}
}
