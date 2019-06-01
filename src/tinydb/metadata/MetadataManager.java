package tinydb.metadata;

import java.util.ArrayList;
import java.util.Map;

import tinydb.record.Table;

public class MetadataManager {
	private static String dbname;

	private static StatManager sm;
	private static TableManager tm;
	private static IndexManager im;

	public MetadataManager(boolean isNew, String dbname) {
		tm = new TableManager(isNew);
		sm = new StatManager(tm);
		im  = new IndexManager(isNew, tm);
		setDBname(dbname);
	}

	public String dbname() {
		return dbname;
	}

	public TableManager tableManager() {
		return tm;
	}

	public void setDBname(String dbname) {
		MetadataManager.dbname = dbname;
	}

	public ArrayList<String> getTableNames() {
		return tm.getTableNames();
	}

	public void deleteTable(String tblname) {
		tm.deleteTable(tblname);
		im.deleteIndex(tblname);
	}

	public Table getTableInfo(String tblname) {
		return tm.getTable(tblname);
	}

	public StatInfo getStatInfo(String tblname, Table tb) {
		return sm.getStatInfo(tblname, tb);
	}

	public void createIndex(String idxname, String tblname, String fldname) {
		im.createIndex(idxname, tblname, fldname);
	}

	// FIXME
	public Map<String, IndexInfo> getIndexInfo(String tblname) {
		// return im.getIndexInfo(tblname);
		return im.getIndexInfo(tblname);
	}
}
