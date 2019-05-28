package tinydb.parse;

public class ShowTablesData {
	private String dbname;

	public ShowTablesData(String dbname) {
	      this.dbname = dbname;
	   }

	public String dbName() {
		return dbname;
	}
}
