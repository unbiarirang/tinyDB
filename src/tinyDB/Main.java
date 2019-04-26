package tinyDB;
import java.util.ArrayList;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
    	// Get DBManager
    	DBManager dbm = DBManager.getDBManager();
    	
    	// Create database
    	Database  db1 = new Database("test-db1", 1);
    	Database  db2 = new Database("test-db2", 1);
    	
    	// Load one database
    	dbm.setDatabase(db1);
    	
    	// Create table
    	Table newTable = dbm.createTable("test-table3", 1);
    	//// Read .script file
    	//ArrayList<String> tmp = newTable.store.readTableDataDisk();
    	//tmp.forEach(line -> System.out.println(line));
    	//// Write to .script file
    	//newTable.store.writeTableDataDisk(tmp);
    	
    	// Delete table
    	dbm.deleteTable("test-table1");
    	dbm.deleteTable("test-table2");
    	
    	// Switch from d1 to db2
    	dbm.setDatabase(db2);
    }
}
