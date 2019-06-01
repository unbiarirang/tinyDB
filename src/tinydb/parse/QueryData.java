package tinydb.parse;

import tinydb.exec.expr.Condition;

import java.util.*;

// Data for the SQL SELECT statement.
public class QueryData {
	private Collection<String> lhstables;
	private Collection<String> fields;
	private Collection<String> tables;
	private Condition cond;

	public QueryData(Collection<String> lhstables, Collection<String> fields, Collection<String> tables,
			Condition cond, boolean isNaturalJoin) {
		this.lhstables = lhstables;
		this.fields = fields;
		this.tables = tables;
		this.cond = cond;
		if (isNaturalJoin)
			this.cond.addNaturalJoin(tables);
	}

	public Collection<String> lhstables() {
		return lhstables;
	}

	public Collection<String> fields() {
		return fields;
	}

	public Collection<String> tables() {
		return tables;
	}

	public Condition cond() {
		return cond;
	}
}
