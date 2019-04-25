package tinyDB;

public class Main {
    public static void main(String[] args) {
    	// Get DBManager
    	DBManager dbm = DBManager.getDBManager();
    	
    	// Create database
    	Database  db1 = new Database("test-db1", 1);
    	Database  db2 = new Database("test-db2", 1);
    	
    	// Load one database
    	dbm.setDatabase(db1);
    	
    	// Create table
    	Table newTable = dbm.createTable("test-table3", 1);
    	
    	// Delete table
    	dbm.deleteTable("test-table1");
    	dbm.deleteTable("test-table2");
    	System.out.println("After delete tables: " + dbm.getTableList().size());
    	
    	// Switch from d1 to db2
    	dbm.setDatabase(db2);
    }
}
