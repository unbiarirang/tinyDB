package tinydb.file;

import static tinydb.file.Page.BLOCK_SIZE;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

// File Manager for a database
public class FileManager {
	private File dbDir;
	private boolean isNew;
	private Map<String, FileChannel> openFiles = new HashMap<String, FileChannel>();

	public FileManager(String dbname) {
		String homedir = System.getProperty("user.home");
		dbDir = new File(homedir, dbname);
		isNew = !dbDir.exists();

		// create the directory if the database is new
		if (isNew && !dbDir.mkdir())
			throw new RuntimeException("cannot create " + dbname);

		// remove any leftover temporary tables
		for (String filename : dbDir.list())
			if (filename.startsWith("temp"))
				new File(dbDir, filename).delete();
	}

	// Reads the contents of a disk block into a bytebuffer.
	synchronized void read(Block blk, ByteBuffer bb) {
		try {
			bb.clear();
			FileChannel fc = getFile(blk.fileName());
			fc.read(bb, blk.number() * BLOCK_SIZE);
		} catch (IOException e) {
			throw new RuntimeException("cannot read block " + blk);
		}
	}

	// Writes the contents of a bytebuffer into a disk block.
	synchronized void write(Block blk, ByteBuffer bb) {
		try {
			bb.rewind();
			FileChannel fc = getFile(blk.fileName());
			fc.write(bb, blk.number() * BLOCK_SIZE);
		} catch (IOException e) {
			throw new RuntimeException("cannot write block" + blk);
		}
	}

	// Appends the contents of a bytebuffer to the end of the specified file.
	synchronized Block append(String filename, ByteBuffer bb) {
		int newblknum = size(filename);
		Block blk = new Block(filename, newblknum);
		write(blk, bb);
		return blk;
	}

	// Returns the number of blocks in the specified file.
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

	// Returns the file channel for the specified filename.
	private FileChannel getFile(String filename) throws IOException {
		FileChannel fc = openFiles.get(filename);
		if (fc == null) {
			File dbTable = new File(dbDir, filename);
			RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
			fc = f.getChannel();
			openFiles.put(filename, fc);
		}
		return fc;
	}
	
	public void deleteDatabase(String dbname) {
		String homedir = System.getProperty("user.home");
		File dbdir = new File(homedir, dbname);
		
		if (dbdir.isDirectory()) {
			for (File f : dbdir.listFiles())
				f.delete();
		}
		if (!dbdir.delete())
			System.out.println("Failed to delete database: " + dbname);
		else {
			System.out.println("Succeed to delete database: " + dbname);
			
			File file = new File(homedir, "dbcat");
			File tempfile = new File(homedir, "dbcat.temp");

			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				BufferedWriter bw = new BufferedWriter(new FileWriter(tempfile));
				String lineToRemove = dbname;
				String currentLine;

				while((currentLine = br.readLine()) != null) {
				    // trim newline when comparing with lineToRemove
				    String trimmedLine = currentLine.trim();
				    if (trimmedLine.equals(lineToRemove)) continue;
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
	}
	
	public void deleteTable(String dbname, String tblname) {
		String homedir = System.getProperty("user.home");
		File dbdir = new File(homedir, dbname);
		
		if (dbdir.isDirectory()) {
			for (File f : dbdir.listFiles()) {
				if (f.getName().contentEquals(tblname + ".tbl")) {
					f.delete();
					System.out.println("Succeed to drop table: " + tblname);
					break;
				}
			}
		}
	}
	
	public ArrayList<String> getDatabaseNames() {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "dbcat");
		BufferedReader br;
		
		try {
			br = new BufferedReader(new FileReader(file));
			ArrayList<String> dbnames = new ArrayList<String>();
		    String dbname = br.readLine();
		    while(dbname != null) {
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

	public void recordDatabaseName(String dirname) {
		String homedir = System.getProperty("user.home");
		File file = new File(homedir, "dbcat");

	    try {
	    	BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			bw.append(dirname + System.getProperty("line.separator"));
		    bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
