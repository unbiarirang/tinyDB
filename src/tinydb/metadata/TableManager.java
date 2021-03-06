package tinydb.metadata;

import java.util.ArrayList;

import tinydb.record.*;

// Manage table
public class TableManager {
	// The maximum length of tblname and fldname.
	public static final int MAX_NAME = 16;

	private Table tableCatalog; // Metadata of all tables in the database
	private Table fieldCatalog; // Metadata of all fields of all tables in the database
	private ArrayList<String> tblnames = null;

	public TableManager(boolean isNew) {
		Schema tcatSchema = new Schema();
		tcatSchema.addStringField("tblname", MAX_NAME);
		tcatSchema.addIntField("reclen");
		this.tableCatalog = new Table("tblcat", tcatSchema);

		Schema fcatSchema = new Schema();
		fcatSchema.addStringField("tblname", MAX_NAME);
		fcatSchema.addStringField("fldname", MAX_NAME);
		fcatSchema.addIntField("type");
		fcatSchema.addIntField("reclen");
		fcatSchema.addIntField("offset");
		fcatSchema.addIntField("notnull");
		fcatSchema.addIntField("ispk");
		this.fieldCatalog = new Table("fldcat", fcatSchema);

		// If the database is new, then the two catalog tables are created.
		if (isNew) {
			createTable("tblcat", tcatSchema);
			createTable("fldcat", fcatSchema);
		}
	}

	// Creates a new table having the specified name and schema.
	public Table createTable(String tblname, Schema sch) {
		Table table = new Table(tblname, sch);
		RecordManager rm = new RecordManager(table); // create .tbl file
		rm.close();

		if (isTableExists(tblname))
			return table;

		// Record metadata
		// Insert a record into tblcat
		rm = new RecordManager(tableCatalog);
		rm.insert();
		rm.setString("tblname", tblname);
		rm.setInt("reclen", table.recordLength());
		rm.close();

		// Insert a record into fldcat for each field
		rm = new RecordManager(fieldCatalog);
		for (String fldname : sch.fields()) {
			rm.insert();
			if(rm.currentRid().id() == 0) {
				rm.insert();
			}
			rm.setString("tblname", tblname);
			rm.setString("fldname", fldname);
			rm.setInt("type", sch.type(fldname));
			rm.setInt("reclen", sch.length(fldname));
			rm.setInt("offset", table.offset(fldname));
			rm.setInt("notnull", sch.isNotNull(fldname));
			rm.setInt("ispk", sch.isPk(fldname));

			// if (sch.isPk(fldname) == 1 && sch.getPk() != "")
			// throw new BadSyntaxException("Only one primary key is allowed");
			if (sch.isPk(fldname) == 1)
				table.setPk(fldname);
		}
		rm.close();
		return table;
	}

	// Retrieves the metadata for the specified table
	public Table getTable(String tblname) {
		RecordManager rm = new RecordManager(fieldCatalog);
		Schema sch = new Schema();
		String pk = null;
		while (rm.next()) {
			if (rm.getString("tblname").equals(tblname)) {
				String fldname = rm.getString("fldname");
				int fldtype = rm.getInt("type");
				int fldlen = rm.getInt("reclen");
				boolean notNull = rm.getInt("notnull") == 1 ? true : false;
				boolean isPk = rm.getInt("ispk") == 1 ? true : false;
				if (isPk)
					pk = fldname;
				sch.addField(fldname, fldtype, fldlen, notNull, isPk);
			}
		}
		rm.close();
		return new Table(tblname, sch, pk);
	}

	public ArrayList<String> getTableNames() {
		if (tblnames != null) return tblnames;

		ArrayList<String> tblnames = new ArrayList<String>();
		RecordManager rm = new RecordManager(tableCatalog);

		while (rm.next()) {
			String tblname = rm.getString("tblname");
			if (!tblname.contentEquals("tblcat") && !tblname.contentEquals("fldcat")
					&& !tblname.contentEquals("idxcat"))
				tblnames.add(tblname);
		}
		rm.close();
		return tblnames;
	}

	public boolean isTableExists(String tblname) {
		// Cannot judge whether tblcat and fldcat are exist
		if (tblname.contentEquals("tblcat") || tblname.contentEquals("fldcat"))
			return false;

		return getTableNames().contains(tblname);
	}

	public void deleteTable(String tblname) {
		RecordManager rm = new RecordManager(tableCatalog);
		rm.deleteAll("tblname", tblname);
		rm.close();

		rm = new RecordManager(fieldCatalog);
		rm.deleteAll("tblname", tblname);
		rm.close();
		
		tblnames = null;
	}
}