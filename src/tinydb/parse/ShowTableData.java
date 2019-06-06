package tinydb.parse;

public class ShowTableData {
	private String tblname;

	public ShowTableData(String tblname) {
		this.tblname = tblname;
	}
	
	public String tblName() {
		return tblname;
	}
}
