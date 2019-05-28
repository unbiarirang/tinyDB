package tinydb.record;

import java.util.*;

import tinydb.consts.Types;
import tinydb.record.Schema;
import tinydb.server.DBManager;
import static tinydb.file.Page.*;

public class Table {
	private String tblname;
	private Schema schema;
	private ArrayList<String> fldnames;
	private HashMap<String, Integer> fldtypes;
	private HashMap<String, Integer> fldsizes;
	private HashMap<String, Integer> offsets;
	private String pk;
	private int recordlen;

	public Table(String tblname) {
		this.tblname = tblname;
		this.schema = new Schema();
		this.fldnames = new ArrayList<String>();
		this.fldtypes = new HashMap<String, Integer>();
		this.fldsizes = new HashMap<String, Integer>();
		this.offsets = new HashMap<String, Integer>();
		this.recordlen = 0;
		this.pk = null;
	}

	public Table(String tblname, Schema schema) {
		this.tblname = tblname;
		this.schema = schema;
		initFields();
		initOffset();
	}

	public Table(String tblname, Schema schema, String pk) {
		this.tblname = tblname;
		this.schema = schema;
		initFields();
		initOffset();
		this.pk = pk;
	}

	private void initFields() {
		// init fldnames
		this.fldnames = new ArrayList<String>(schema.fields());

		// init fldtypes
		this.fldtypes = new HashMap<String, Integer>();
		Iterator<String> it1 = fldnames.iterator();
		Iterator<Integer> it2 = schema.types().iterator();
		while (it1.hasNext() && it2.hasNext()) {
			this.fldtypes.put(it1.next(), it2.next());
		}

		// init fldsizes
		this.fldsizes = new HashMap<String, Integer>();
		for (String fldname : fldnames) {
			int type = fldtypes.get(fldname);
			if (type == Types.INTEGER)
				this.fldsizes.put(fldname, INT_SIZE);
			else if (type == Types.LONG)
				this.fldsizes.put(fldname, LONG_SIZE);
			else if (type == Types.FLOAT)
				this.fldsizes.put(fldname, FLOAT_SIZE);
			else if (type == Types.DOUBLE)
				this.fldsizes.put(fldname, DOUBLE_SIZE);
			else
				this.fldsizes.put(fldname, STR_SIZE(schema.getField(fldname).length));
		}
	}

	private void initOffset() {
		this.offsets = new HashMap<String, Integer>();
		this.recordlen = addFields();
	}

	private int addFields() {
		int offset = 0;
		for (String fldname : fldnames) {
			int fldsize = fldsizes.get(fldname);
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

	public HashMap<String, Integer> fldtypes() {
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

	public String pk() {
		return pk;
	}

	public void setPk(String fldname) {
		this.pk = fldname;
	}

	public Schema schema() {
		return schema;
	}

	public int size(String filename) {
		return DBManager.fileManager().size(filename);
	}

	public boolean equals(Table tb) {
		return tblname.contentEquals(tb.tblname) && this.recordlen == tb.recordlen && schema.equals(tb.schema);
	}
}