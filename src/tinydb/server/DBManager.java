package tinydb.server;

import java.util.ArrayList;

import tinydb.auth.AuthManager;
import tinydb.auth.AuthManager.PasswordInfo;
import tinydb.file.FileManager;
import tinydb.metadata.MetadataManager;
import tinydb.metadata.TableManager;
import tinydb.util.PasswordUtils;

// (static class) TinyDB database manager
public class DBManager {
	private static FileManager 		fm;
	private static MetadataManager 	mm;
	private static AuthManager 		am;

	public static void initDB(String dbname) {
		// init file manager
		fm = new FileManager(dbname); // set working directory
		// init authenticate manager
		am = new AuthManager(fm.getUserInfo());
		
		boolean isNew = fm.isNew();
		
		// init metadata manager
		mm = new MetadataManager(isNew, dbname); // set current dbname
		
		if (isNew) {
			fm.recordDatabaseName(dbname);
			System.out.println("creating new database");
		} else
			System.out.println("recovering existing database");
	}

	public static FileManager fileManager() {
		return fm;
	}

	public static MetadataManager metadataManager() {
		return mm;
	}

	public static AuthManager authManager() {
		return am;
	}
	
	public static TableManager tableManager() {
		return mm.tableManager();
	}

	public static void dropDatabase(String dbname) throws Exception {
//		if (mm.dbname().contentEquals(dbname))
//			throw new Exception("Cannot drop current database!");

		fm.deleteDatabase(dbname);	// delete dbname directory and all related metadata
	}

	public static void dropTable(String tblname) {
		fm.deleteTable(mm.dbname(), tblname); 	// delete tblname.tbl file
		mm.deleteTable(tblname);				// delete metadata from tblcat and fldcat
		fm.deleteUserInfos(mm.dbname(), tblname); // delete metadata from usercat
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
	
	public static void createUser(String username, String password) {
		String salt = PasswordUtils.getSalt();
		String pwhash = PasswordUtils.generateSecurePassword(password, salt);
		// Add password info
		am.addPasswordInfo(username, salt, pwhash);
		fm.recordPasswordInfo(username, salt, pwhash);
	}
	
	public static void deleteUser(String username) {
		PasswordInfo pwinfo = am.getPasswordInfo(username);
		String salt = pwinfo.x;
		String pwhash = pwinfo.y;
		// Delete password info
		am.removePasswordInfo(username, salt, pwhash);
		fm.deletePasswordInfo(username, salt, pwhash);
	}
	
	public static boolean verifyUser(String username, String password) {
		return am.authenticate(username, password);
	}

	public static void addUserRole(String username, String tblname, String opname) {
		String role = String.join(" ", username, mm.dbname(), tblname, opname);
		am.addUserRole(role);
		fm.recordUserRole(role);
	}
	
	public static void removeUserRole(String username, String tblname, String opname) {
		String role = String.join(" ", username, mm.dbname(), tblname, opname);
		am.removeUserRole(role);
		fm.deleteUserRole(role);
	}
}