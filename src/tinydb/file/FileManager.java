package tinydb.file;

import static tinydb.file.Page.BLOCK_SIZE;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import tinydb.auth.AuthManager.*;
import tinydb.util.Tuple;

// File Manager for a database
public class FileManager {
	private File dbdir;
	private boolean isNew;
	private Map<String, FileChannel> openFiles = new HashMap<String, FileChannel>();

	public FileManager(String dbname) {
		String homedir = System.getProperty("user.home");
		dbdir = new File(homedir, dbname);
		isNew = !dbdir.exists();

		// Create the directory if the database is new
		if (isNew && !dbdir.mkdir())
			throw new RuntimeException("cannot create " + dbname);

		// Remove any leftover temporary tables
		for (String filename : dbdir.list())
			if (filename.startsWith("temp"))
				new File(dbdir, filename).delete();
	}

	// Read the contents of a disk block into a bytebuffer.
	synchronized void read(Block blk, ByteBuffer bb) {
		try {
			bb.clear();
			FileChannel fc = getFile(blk.fileName());
			fc.read(bb, blk.number() * BLOCK_SIZE);
		} catch (IOException e) {
			throw new RuntimeException("cannot read block " + blk);
		}
	}

	// Write the contents of a bytebuffer into a disk block.
	synchronized void write(Block blk, ByteBuffer bb) {
		try {
			bb.rewind();
			FileChannel fc = getFile(blk.fileName());
			fc.write(bb, blk.number() * BLOCK_SIZE);
		} catch (IOException e) {
			throw new RuntimeException("cannot write block" + blk);
		}
	}

	// Append the contents of a bytebuffer to the end of the specified file.
	synchronized Block append(String filename, ByteBuffer bb) {
		int newblknum = size(filename);
		Block blk = new Block(filename, newblknum);
		write(blk, bb);
		return blk;
	}

	// Return the number of blocks in the specified file.
	public synchronized int size(String filename) {
		try {
			FileChannel fc = getFile(filename);
			return (int) (fc.size() / BLOCK_SIZE);
		} catch (IOException e) {
			throw new RuntimeException("cannot access " + filename);
		}
	}

	// Whether the file manager had to create a new database directory.
	public boolean isNew() {
		return isNew;
	}

	// Return the file channel for the specified filename.
	private FileChannel getFile(String filename) throws IOException {
		FileChannel fc = openFiles.get(filename);
		if (fc == null) {
			File dbTable = new File(dbdir, filename);
			RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
			fc = f.getChannel();
			openFiles.put(filename, fc);
		}
		return fc;
	}

	public void deleteDatabase(String dbname) {
		String homedir = System.getProperty("user.home");
		File dbdir = new File(homedir, dbname);

		if (dbdir.isDirectory()) {	// delete dbname directory recursively
			for (File f : dbdir.listFiles())
				f.delete();
		}
		if (!dbdir.delete())
			System.out.println("Failed to delete database: " + dbname);
		else {
			System.out.println("Succeed to delete database: " + dbname);
			deleteDatabaseName(dbname); // delete metadata from dbcat
			deleteUserInfos(dbname);	// delete metadata from usercat
			openFiles.clear();
		}
	}

	public void deleteTable(String dbname, String tblname) {
		String homedir = System.getProperty("user.home");
		File dbdir = new File(homedir, dbname);

		if (dbdir.isDirectory()) {
			for (File f : dbdir.listFiles()) {
				String fname = f.getName();
				if (fname.contentEquals(tblname + ".tbl") || fname.contains(tblname + "pk")) {
					f.delete();
					System.out.println("Succeed to drop table: " + fname);
				}
			}
		}
		openFiles.clear();
	}

	public ArrayList<String> getDatabaseNames() {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "dbcat");
		BufferedReader br;

		try {
			br = new BufferedReader(new FileReader(file));
			ArrayList<String> dbnames = new ArrayList<String>();
			String dbname = br.readLine();
			while (dbname != null) {
				dbnames.add(dbname);
				dbname = br.readLine();
			}
			br.close();
			return dbnames;
		} catch (FileNotFoundException e) {
			e.getStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void recordDatabaseName(String dbname) {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "dbcat");

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			bw.append(dbname + System.getProperty("line.separator"));
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteDatabaseName(String dbname) {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "dbcat");
		File tempfile = new File(homedir, "dbcat.temp");

		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			BufferedWriter bw = new BufferedWriter(new FileWriter(tempfile));
			String lineToRemove = dbname;
			String currentLine;

			while ((currentLine = br.readLine()) != null) {
				// trim newline when comparing with lineToRemove
				String trimmedLine = currentLine.trim();
				if (trimmedLine.equals(lineToRemove))
					continue;
				bw.write(currentLine + System.getProperty("line.separator"));
			}
			br.close();
			bw.close();

			if (tempfile.renameTo(file))
				System.out.println(dbname + " was removed from the dbcat");
			else
				System.out.println(dbname + " was failed to removed from the dbcat");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Tuple<Privileges, PasswordInfos> getUserInfo() {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "usercat");
		BufferedReader br;

		Privileges privileges = new Privileges();
		PasswordInfos pwinfos = new PasswordInfos();

		try {
			br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			
			while (line != null) {
				String[] tokens = line.split(" ");
				
				if (tokens.length == 3) 	 // username - salt - pwhash
					pwinfos.put(tokens[0], new PasswordInfo(tokens[1], tokens[2]));
				else if (tokens.length == 4) // username - dbname - tblname - opname
					privileges.add(line);
				else { 
					br.close();
					throw new IOException("Wrong file format!");
				}
				
				line = br.readLine();
			}
			br.close();
			return new Tuple<Privileges, PasswordInfos>(privileges, pwinfos);
		} catch (FileNotFoundException e) {
			e.getStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void recordUserInfo(String line) {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "usercat");

		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			bw.append(line + System.getProperty("line.separator"));
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void recordUserPrivilege(String privilege) {
		recordUserInfo(privilege);
	}	

	public void recordPasswordInfo(String username, String salt, String pwhash) {
		recordUserInfo(String.join(" ", username, salt, pwhash));
	}
	

	public void deleteUserInfos(String dbname) {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "usercat");
		File tempfile = new File(homedir, "usercat.temp");

		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			BufferedWriter bw = new BufferedWriter(new FileWriter(tempfile));
			String currentLine;

			while ((currentLine = br.readLine()) != null) {
				// trim newline when comparing with lineToRemove
				String trimmedLine = currentLine.trim();
				String dbnameLine = trimmedLine.split(" ")[1];
				if (dbnameLine.equals(dbname))
					continue;
				bw.write(currentLine + System.getProperty("line.separator"));
			}
			br.close();
			bw.close();

			if (tempfile.renameTo(file))
				System.out.println(dbname + " was removed from the usercat");
			else
				System.out.println(dbname + " was failed to removed from the usercat");

		} catch (FileNotFoundException e) {
			// usercat not exists. Do not have to delete
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteUserInfos(String dbname, String tblname) {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "usercat");
		File tempfile = new File(homedir, "usercat.temp");

		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			BufferedWriter bw = new BufferedWriter(new FileWriter(tempfile));
			String currentLine;

			while ((currentLine = br.readLine()) != null) {
				// trim newline when comparing with lineToRemove
				String[] tokens = currentLine.trim().split(" ");
				String dbnameLine = tokens[1];
				String tblnameLine = tokens[2];
				if (dbnameLine.equals(dbname) && tblnameLine.equals(tblname)) // delete the data
					continue;
				bw.write(currentLine + System.getProperty("line.separator"));
			}
			br.close();
			bw.close();

			if (tempfile.renameTo(file))
				System.out.println(dbname + " was removed from the usercat");
			else
				System.out.println(dbname + " was failed to removed from the usercat");

		} catch (FileNotFoundException e) {
			// usercat not exists. Do not have to delete
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteUserInfo(String line) {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "usercat");
		File tempfile = new File(homedir, "usercat.temp");

		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			BufferedWriter bw = new BufferedWriter(new FileWriter(tempfile));
			String lineToRemove = line;
			String currentLine;

			while ((currentLine = br.readLine()) != null) {
				// trim newline when comparing with lineToRemove
				String trimmedLine = currentLine.trim();
				if (trimmedLine.equals(lineToRemove))
					continue;
				bw.write(currentLine + System.getProperty("line.separator"));
			}
			br.close();
			bw.close();

			if (tempfile.renameTo(file))
				System.out.println(line + " was removed from the usercat");
			else
				System.out.println(line + " was failed to removed from the usercat");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteUserPrivilege(String privilege) {
		deleteUserInfo(privilege);
	}
	
	public void deletePasswordInfo(String username, String salt, String pwhash) {
		deleteUserInfo(String.join(" ", username, salt, pwhash));
	}
}
