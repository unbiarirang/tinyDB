package tinydb.parse;

import tinydb.exec.*;
import tinydb.exec.expr.Condition;

import java.util.*;

// Data for the SQL SELECT statement.
public class QueryData {
	private Collection<String> lhstables;
	private Collection<String> fields;
	private Collection<String> tables;
	private Condition cond;

	public QueryData(Collection<String> lhstables, Collection<String> fields, Collection<String> tables,
			Condition cond) {
		this.lhstables = lhstables;
		this.fields = fields;
		this.tables = tables;
		this.cond = cond;
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

	public String toString() {
		String result = "select ";
		for (String fldname : fields)
			result += fldname + ", ";
		result = result.substring(0, result.length() - 2); // remove final comma
		result += " from ";
		for (String tblname : tables)
			result += tblname + ", ";
		result = result.substring(0, result.length() - 2); // remove final comma
		String predstring = cond.toString();
		if (!predstring.equals(""))
			result += " where " + predstring;
		return result;
	}
}
