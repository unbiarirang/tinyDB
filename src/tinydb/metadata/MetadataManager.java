package tinydb.metadata;

import java.util.ArrayList;

import tinydb.metadata.TableManager;

public class MetadataManager {
	private static String 	dbname;
	
	private static TableManager tm;

	public MetadataManager(boolean isNew, String dbname) {
		tm = new TableManager(isNew);
		setDBname(dbname);
	}
	
	public String 		dbname() 		{ return dbname; }
	public TableManager tableManager() 	{ return tm; }
	
	public void setDBname(String dbname) {
		MetadataManager.dbname = dbname;
	}
	
	public ArrayList<String> getTableNames() {
		return tm.getTableNames();
	}

	public void deleteTable(String tblname) {
		tm.deleteTable(tblname);
	}
}
