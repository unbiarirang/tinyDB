package tinydb.parse;

import tinydb.record.Schema;

public class CreateTableData {
	private String tblname;
	private Schema schema;

	public CreateTableData(String tblname, Schema schema) {
		this.tblname = tblname;
		this.schema = schema;
	}

	public String tableName() {
		return tblname;
	}

	public Schema newSchema() {
		return schema;
	}
}
