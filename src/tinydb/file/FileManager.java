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
}
