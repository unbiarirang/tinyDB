package tinydb.auth;

import java.util.HashMap;
import java.util.HashSet;

import tinydb.util.PasswordUtils;
import tinydb.util.Tuple;

public class AuthManager {
	private static final int SALT_LEN = 30;

	private static Privileges 		privileges = new Privileges();
	private static PasswordInfos 	pwinfos = new PasswordInfos(); // key: username, val: PasswordInfo

	public AuthManager(Tuple<Privileges, PasswordInfos> userInfo) {
		if (userInfo == null) return;

		AuthManager.privileges = userInfo.x;
		AuthManager.pwinfos = userInfo.y;
	}

	public boolean authenticate(String username, String password) {
		PasswordInfo t = pwinfos.get(username);
		if (t == null) {
			System.out.println("User not exists!");
			return false;
		}
		
		String salt = t.x;	 // salt stored in usercat 
		String pwhash = t.y; // encrypted and Base64 encoded password read from usercat
        
        boolean match = PasswordUtils.verifyPassword(password, pwhash, salt);
        if (match) return true;
        else return false;
	}

	public void addUserRole(String role) {
		privileges.add(role);
	}
	
	public void removeUserRole(String role) {
		privileges.remove(role);
	}

	public boolean checkUserRole(String role) {
		return privileges.contains(role);
	}
	
	public void addPasswordInfo(String username, String salt, String pwhash) {
		AuthManager.pwinfos.put(username, new PasswordInfo(salt, pwhash));
	}
	
	public void removePasswordInfo(String username, String salt, String pwhash) {
		AuthManager.pwinfos.remove(username);
	}
	
	public PasswordInfo getPasswordInfo(String username) {
		return pwinfos.get(username);
	}
	
	public String getSalt(String username) {
		return pwinfos.get(username).x;
	}
	
	public String getPasswordHash(String username) {
		return pwinfos.get(username).y;
	}
	
	public static class Privileges extends HashSet<String> {
		public Privileges() { super(); }
	}
	
	// x: salt, y: pwhash
	public static class PasswordInfo extends Tuple<String, String> {
		public PasswordInfo(String salt, String pwhash) { super(salt, pwhash); }
	}
	
	// key: username, val: PasswordInfo
	public static class PasswordInfos extends HashMap<String, PasswordInfo> {
	    public PasswordInfos() { super(); }
	}
}