package tinydb.plan;

import tinydb.server.DBManager;
import tinydb.exec.*;
import tinydb.metadata.*;
import tinydb.record.*;

import java.util.HashMap;
import java.util.Map;

// The most basic Plan, Plan for a corresponding table
public class TablePlan implements Plan {
	private Table tb;
	private StatInfo si;

	public TablePlan(String tblname) {
		tb = DBManager.metadataManager().getTableInfo(tblname);
		si = DBManager.metadataManager().getStatInfo(tblname, tb);
	}

	// Plan methods //

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
		if (blocksAccessed() < 1000)
			return si.distinctValues(fldname);
		
		return distinctValuesChao1(fldname);
	}

	public Schema schema() {
		return tb.schema();
	}
	
	// Chao1 sample based distinct value estimator
	// D = d + f1^2 / 2*f2
	private int distinctValuesChao1(String fldname) {
		RecordManager rm = new RecordManager(tb);
		int gap = blocksAccessed() / 100;
		HashMap<Object, Integer> counts = rm.scanCount(fldname, gap);
		int f1 = 0, f2 = 0;

		for (Map.Entry<Object, Integer> entry : counts.entrySet()) {
			if (entry.getValue() == 1) f1++;
			if (entry.getValue() == 2) f2++;
		}
		return counts.size() + f1*f1 / 2*f2;
	}
}
