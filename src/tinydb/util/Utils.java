package tinydb.util;

import java.util.ArrayList;
import java.util.List;

public class Utils {
	public static <T> List<T> intersection(List<T> list1, List<T> list2) {
		List<T> list = new ArrayList<T>();

		for (T t : list1) {
			if (list2.contains(t)) {
				list.add(t);
			}
		}

		return list;
	}

	public static String parseFirstCmd(String cmd) {
		String fstCmd = null;
		int start = 0;

		for (int i = 0; i < cmd.length(); i++) {
			if (cmd.charAt(i) == ' ') {
				fstCmd = cmd.substring(start, i);
				break;
			} else if (cmd.charAt(i) == '\r' || cmd.charAt(i) == '\n')
				start++;
		}
		return fstCmd;
	}
	
	public static String getDB(String msg) {
		
		
		return msg;
	}
	
	public static String[] getInfo(String msg) {
		String id = null, pw = null, db = null;
		String[] info = new String[3];
		int i;
		int cnt = 0;
		int fst = 0;
		for (i = 6; i < msg.length(); i++) {
			if (msg.charAt(i) == '\n') {
				cnt++;
				if (cnt == 1) {
					db = msg.substring(6, i);
					info[0] = db;
					fst = i + 1;
				}
				else if (cnt == 2){
					id = msg.substring(fst, i);
					info[1] = id;
					fst = i + 1;
				}
				else {
					pw = msg.substring(fst, i);
					info[2] = pw;
					break;
				}
			}
		}

		return info;
	}
}
