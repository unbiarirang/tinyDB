package tinydb.parse;

public class DropTableData {
	private String tblname;

	public DropTableData(String tblname) {
	      this.tblname = tblname;
	   }

	public String tblName() {
		return tblname;
	}
}
