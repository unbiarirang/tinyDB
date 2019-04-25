package tinyDB;
import java.util.ArrayList;
import java.util.Arrays;

public class Database {
    int		dbID;
    String  dbName;

	 public Database(String dbName, int dbID) {
		 this.dbID = dbID;
		 this.dbName = dbName;
	 }
	 
	 public ArrayList<Table> loadTables() {
		 System.out.println("Load tables of database " + dbName);
		 // Read table info from disk
		 // Table[] tables = IOmanager.read(...)
		 Table[] tables = { 
				 new Table(this, "test-table1", 1),           
				 new Table(this, "test-table2", 1)
		 };

		 return new ArrayList<Table>(Arrays.asList(tables));
	 }
	 
	 public void save() {
		 System.out.println("Save and quit database " + dbName);
	 }
}