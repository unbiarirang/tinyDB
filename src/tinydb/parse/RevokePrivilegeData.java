package tinydb.parse;

public class RevokePrivilegeData {
	private String privilege;
	private String tblname;
	private String username;
	
	RevokePrivilegeData (String privilege, String tblname, String username) {
		this.privilege = privilege;
		this.tblname  = tblname;
		this.username = username;
	}

	public String privilege() {
		return privilege;
	}
	
	public String tblname() {
		return tblname;
	}

	public String username() {
		return username;
	}
}