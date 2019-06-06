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

	public static Tuple<String, String> getIDandPW(String msg) {
		String id = null, pw = null;

		int i;
		for (i = 6; i < msg.length(); i++) {
			if (msg.charAt(i) == ' ') {
				id = msg.substring(6, i);
				break;
			}
		}

		pw = msg.substring(i + 1, msg.length() - 1);

		return new Tuple<String, String>(id, pw);
	}
}
