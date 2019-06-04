package tinydb.parse;

public class DropDatabaseData {
	private String dbname;

	public DropDatabaseData(String dbname) {
		this.dbname = dbname;
	}

	public String dbName() {
		return dbname;
	}
}
