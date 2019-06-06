package tinydb.parse;

public class ShowDatabaseData {
	private String dbname;

	public ShowDatabaseData(String dbname) {
	      this.dbname = dbname;
	   }

	public String dbName() {
		return dbname;
	}
}
