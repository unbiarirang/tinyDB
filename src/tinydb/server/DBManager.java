package tinydb.server;

import java.util.ArrayList;

import tinydb.file.FileManager;
import tinydb.metadata.MetadataManager;
import tinydb.metadata.TableManager;

// (static class) TinyDB database manager
public class DBManager {
	private static FileManager fm;
	private static MetadataManager mm;

	public static void initDB(String dbname) {
		// init file manager
		fm = new FileManager(dbname);
		boolean isNew = fm.isNew();
		if (isNew) {
			fm.recordDatabaseName(dbname);
			System.out.println("creating new database");
		} else
			System.out.println("recovering existing database");

		// init metadata manager
		mm = new MetadataManager(isNew);
		mm.setDBname(dbname);
	}

	public static FileManager fileManager() {
		return fm;
	}

	public static MetadataManager metadataManager() {
		return mm;
	}
	
	public static TableManager tableManager() {
		return mm.tableManager();
	}

	public static void dropDatabase(String dbname) throws Exception {
		if (mm.dbname().contentEquals(dbname))
			throw new Exception("Cannot drop current database!");
		
		fm.deleteDatabase(dbname);
	}

	public static void dropTable(String tblname) {
		fm.deleteTable(mm.dbname(), tblname); 	// delete tblname.tbl file
		mm.deleteTable(tblname);				// delete data from metadata
	}

	public static ArrayList<String> showDatabaseTables(String dbname) {
		String oldname = mm.dbname();

		if (dbname == oldname)
			return mm.getTableNames(); // Do not need to change db

		initDB(dbname);
		ArrayList<String> tableNames = mm.getTableNames();
		initDB(oldname);
		return tableNames;
	}

	public static ArrayList<String> showDatabases() {
		return fm.getDatabaseNames();
	}
}