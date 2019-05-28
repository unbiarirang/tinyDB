package tinydb.plan;

import tinydb.server.DBManager;
import tinydb.exec.*;
import tinydb.metadata.*;
import tinydb.record.*;

public class TablePlan implements Plan {
	private Table tb;
	private StatInfo si;

	public TablePlan(String tblname) {
		tb = DBManager.metadataManager().getTableInfo(tblname);
		si = DBManager.metadataManager().getStatInfo(tblname, tb);
	}

	public Exec exec() {
		return new TableExec(tb);
	}

	public int blocksAccessed() {
		return si.blocksAccessed();
	}

	public int recordsOutput() {
		return si.recordsOutput();
	}

	public int distinctValues(String fldname) {
		return si.distinctValues(fldname);
	}

	public Schema schema() {
		return tb.schema();
	}
}
