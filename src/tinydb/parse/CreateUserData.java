package tinydb.parse;

public class CreateUserData {
	private String username;
	private String userpw;
	
	CreateUserData (String username, String userpw) {
		this.username = username;
		this.userpw = userpw;
	}
	
	public String username() {
		return username;
	}
	
	public String userpw() {
		return userpw;
	}
}