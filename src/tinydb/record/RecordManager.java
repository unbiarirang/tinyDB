package tinydb.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import tinydb.file.Block;
import tinydb.file.Page;

// Record Manager of a file
public class RecordManager {
	private Table 	tb;
	private String 	filename;
	private Record 	rc;
	private int 	curblkid;

	public RecordManager(Table tb) {
		this.tb = tb;
		filename = tb.fileName();
		moveTo(0);
		if (tb.size(filename) == 0)
			insert();
	}

	// Moves to the next record. Returns false if there is no next record.
	public boolean next() {
		while (true) {
			if (rc.next())
				return true;
			if (atLastBlock())
				return false;
			moveTo(curblkid + 1);
		}
	}

	// Returns the value of the specified field in the current record.
	public int getInt(String fldname) {
		return rc.getInt(fldname);
	}

	public long getLong(String fldname) {
		return rc.getLong(fldname);
	}

	public float getFloat(String fldname) {
		return rc.getFloat(fldname);
	}

	public double getDouble(String fldname) {
		return rc.getDouble(fldname);
	}

	public String getString(String fldname) {
		return rc.getString(fldname);
	}

	// Sets the value of the specified field in the current record.
	public void setInt(String fldname, int val) {
		rc.setInt(fldname, val);
	}

	public void setLong(String fldname, long val) {
		rc.setLong(fldname, val);
	}

	public void setFloat(String fldname, float val) {
		rc.setFloat(fldname, val);
	}

	public void setDouble(String fldname, double val) {
		rc.setDouble(fldname, val);
	}

	public void setString(String fldname, String val) {
		rc.setString(fldname, val);
	}

	// Inserts a new empty record.
	public void insert() {
		while (!rc.insert()) {
			if (atLastBlock())
				appendBlock();
			moveTo(curblkid + 1);
		}
	}

	// Inserts a new record with a row data to the page.
	public void insert(ArrayList<String> fldnames, ArrayList<Object> row) throws IOException {
		insert();
		rc.insert(fldnames, row);
	}

	// Stop write to the block.
	public void close() {
		rc.write();
		rc.close();
	}

	// Delete current record.
	public void delete() {
		rc.delete();
		rc.write();
	}

	// Scan all records and delete records that specifies the condition (fldname=vlaue).
	public void delete(String fldname, Object value) {
		moveToFirst();

		if (value instanceof Integer) {
			while (next()) {
				if (rc.getInt(fldname) == ((Integer) value).intValue())
					delete();
			}
		} else if (value instanceof Long) {
			while (next()) {
				if (rc.getLong(fldname) == ((Long) value).longValue())
					delete();
			}
		} else if (value instanceof Float) {
			while (next()) {
				if (rc.getFloat(fldname) == ((Float) value).floatValue())
					delete();
			}
		} else if (value instanceof Double) {
			while (next()) {
				if (rc.getDouble(fldname) == ((Double) value).doubleValue())
					delete();
			}
		} else {
			while (next()) {
				if (rc.getString(fldname).contentEquals(((String) value).toString()))
					delete();
			}
		}
	}

	private void moveTo(int b) {
		curblkid = b;
		Block blk = new Block(filename, curblkid);
		rc = new Record(tb, blk);
	}
	
	public void moveToFirst() {
		moveTo(0);
	}

	private boolean atLastBlock() {
		return curblkid == tb.size(filename) - 1;
	}

	private void appendBlock() {
		Page p = new Page();
		p.append(filename);
	}

	// Scan and print all records
	public void scanAll() {
		ArrayList<String> fldnames = tb.fldnames();
		Iterator<String> it = fldnames.iterator();
		while (it.hasNext())
			System.out.print(it.next() + "\t");
		System.out.println();

		moveToFirst();
		while (next()) {
			rc.print();
		}
	}
	
	// Scan all records and print records that specifies the condition (fldname=vlaue)
	public void scan(String fldname, Object value) {
		moveToFirst();
		
		ArrayList<String> fldnames = tb.fldnames();
		Iterator<String> it = fldnames.iterator();
		while (it.hasNext())
			System.out.print(it.next() + "\t");
		System.out.println();

		if (value instanceof Integer) {
			while (next()) {
				if (rc.getInt(fldname) == ((Integer) value).intValue())
					rc.print();
			}
		} else if (value instanceof Long) {
			while (next()) {
				if (rc.getLong(fldname) == ((Long) value).longValue())
					rc.print();
			}
		} else if (value instanceof Float) {
			while (next()) {
				if (rc.getFloat(fldname) == ((Float) value).floatValue())
					rc.print();
			}
		} else if (value instanceof Double) {
			while (next()) {
				if (rc.getDouble(fldname) == ((Double) value).doubleValue())
					rc.print();
			}
		} else {
			while (next()) {
				if (rc.getString(fldname) == ((String) value).toString())
					rc.print();
			}
		}
	}
}