package tinyDB;
import java.util.ArrayList;

// Singleton
public class DBManager {
	private static DBManager 	dbm;
	private ArrayList<Table> 	tableList;
	private Database 			db;
	
	private DBManager() {
		this.db = null;
		this.tableList = null;
	}
	
	public static DBManager getDBManager() {
		if (dbm != null) return dbm;
		
		dbm = new DBManager();
		return dbm;
	}
	
	public Database getDatabase() {
		return this.db;
	}
	
	public void setDatabase(Database db) {
		System.out.println("Load database: " + db.dbName);
		// Switch the database
		// Save and quit the previous database
		if (this.db != null) {
			this.db.save();
		}

		this.tableList = db.loadTables();
		this.db = db;

    	System.out.println("After load database: " + this.tableList.size());
		return;
	}

	public ArrayList<Table> getTableList() {
		return this.tableList;
	}
	
	public Table createTable(String tableName, int tableType) {
		Table newTable = new Table(this.db, tableName, tableType);
		this.tableList.add(newTable);
		
    	System.out.println("After create new table: " + this.tableList.size());
		return newTable;
	}
	
	public boolean deleteTable(String tableName) {
		Table toBeDeleted = null;
		for (Table table : this.tableList) {
			if (table.tableName == tableName) {
				toBeDeleted = table;
				break;
			}
		}

		return this.tableList.remove(toBeDeleted);
	}
}
