package tinydb.server;

import tinydb.file.FileManager;

// (static class) TinyDB database manager
public class DBManager {
	private static FileManager fm;

	public static void initDB(String dbname) {
		fm = new FileManager(dbname);
	}

	public static FileManager fileManager() {
		return fm;
	}
}