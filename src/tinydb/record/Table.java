package tinydb.record;


import java.util.*;

import tinydb.server.DBManager;

public class Table {
	private String tblname;
	private ArrayList<String> fldnames;
	private HashMap<String, String> fldtypes;
	private HashMap<String, Integer> fldsizes;
	private HashMap<String, Integer> offsets;
	private int recordlen;

	public Table(String tblname) {
		this.tblname = tblname;
		this.fldnames = new ArrayList<String>();
		this.fldtypes = new HashMap<String, String>();
		this.fldsizes = new HashMap<String, Integer>();
		this.offsets = new HashMap<String, Integer>();
		this.recordlen = 0;
	}

	public Table(String tblname, ArrayList<String> fldnames, HashMap<String, String> fldtypes,
			HashMap<String, Integer> fldsizes) {
		this.tblname = tblname;
		this.fldnames = fldnames;
		this.fldtypes = fldtypes;
		this.fldsizes = fldsizes;
		initOffset();
	}
	
	private void initOffset() {
		this.offsets = new HashMap<String, Integer>();
		this.recordlen = addFields();
	}

	private int addFields() {
		int offset = 0;
		for (String fldname : fldnames) {
			int    fldsize = fldsizes.get(fldname);
			offset = addField(fldname, offset, fldsize);
		}
		return offset;
	}

	private int addField(String fldname, int offset, int len) {
		this.offsets.put(fldname, offset);
		offset += len;
		return offset;
	}

	public ArrayList<String> fldnames() {
		return fldnames;
	}
	
	public HashMap<String, String> fldtypes() {
		return fldtypes;
	}
	
	public String fileName() {
		return tblname + ".tbl";
	}

	// Returns the offset of a specified field within a record
	public int offset(String fldname) {
		return offsets.get(fldname);
	}

	public int recordLength() {
		return recordlen;
	}

	public int size(String filename) {
		return DBManager.fileManager().size(filename);
	}
}