package tinyDB;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.FileOutputStream;
import java.io.IOException;

// Singleton? or just use static methods?
public class IOManager {
	private static IOManager 	iom;
	
	private IOManager() {
	}

	public static IOManager getIOManager() {
		if (iom != null) return iom;
		
		iom = new IOManager();
		return iom;
	}

	public static void touchFile(String filename) throws IOException {
    	FileOutputStream output = null;
		output = new FileOutputStream(filename);
		output.close();
	}
	
	public static ArrayList<String> readLineByLine(String filename) throws IOException {
		ArrayList<String> lines = new ArrayList<String>();
	    BufferedReader br = new BufferedReader(new FileReader(filename));
	    
	    while(true) {
	        String line = br.readLine();
	        if (line == null) break;
	        lines.add(line);
	    }
	    
	    br.close();
	    return lines;
	}
	
    public static void writeLineByLine(String filename, ArrayList<String> lines) throws IOException {
    	PrintWriter pw = new PrintWriter(filename);
        lines.forEach(line -> pw.println(line));
        pw.close();
    }
    
    public static void appendLineByLine(String filename, ArrayList<String> lines) throws IOException {
    	PrintWriter pw = new PrintWriter(new FileWriter(filename, true));
        lines.forEach(line -> pw.println(line));
        pw.close();
    }
}