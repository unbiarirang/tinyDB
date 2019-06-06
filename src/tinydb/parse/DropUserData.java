package tinydb.parse;

public class DropUserData {
	private String username;
	
	DropUserData (String username) {
		this.username = username;
	}
	
	public String username() {
		return username;
	}
}