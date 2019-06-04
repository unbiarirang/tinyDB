package tinydb.parse;

import java.util.*;

import tinydb.exec.consts.Constant;

public class InsertData {
	private String tblname;
	private List<String> fields;
	private List<Constant> vals;
	private boolean isAll;

	public InsertData(String tblname, List<String> fields, List<Constant> vals, boolean isAll) {
		this.tblname = tblname;
		this.fields = fields;
		this.vals = vals;
		this.isAll = isAll; // whether is applies to all fields
	}

	public String tableName() {
		return tblname;
	}

	public List<String> fields() {
		return fields;
	}

	public List<Constant> vals() {
		return vals;
	}

	public boolean isAll() {
		return isAll;
	}
}
