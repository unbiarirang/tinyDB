package tinydb.metadata;

import static tinydb.metadata.TableManager.MAX_NAME;
import tinydb.record.*;
import java.util.*;

public class IndexManager {
	private Table tb;

	public IndexManager(boolean isnew, TableManager tm) {
		if (isnew) {
			Schema sch = new Schema();
			sch.addStringField("indexname", MAX_NAME);
			sch.addStringField("tablename", MAX_NAME);
			sch.addStringField("fieldname", MAX_NAME);
			tm.createTable("idxcat", sch);
		}
		tb = tm.getTable("idxcat");
	}

	public void createIndex(String idxname, String tblname, String fldname) {
		RecordManager rm = new RecordManager(tb);
		rm.insert();
		rm.setString("indexname", idxname);
		rm.setString("tablename", tblname);
		rm.setString("fieldname", fldname);
		rm.close();
	}
	
	public void deleteIndex(String tblname) {
		RecordManager rm = new RecordManager(tb);
		rm.deleteAll("tablename", tblname);
		rm.close();
	}

	public Map<String, IndexInfo> getIndexInfo(String tblname) {
		Map<String, IndexInfo> result = new HashMap<String, IndexInfo>();
		RecordManager rm = new RecordManager(tb);
		while (rm.next())
			if (rm.getString("tablename").equals(tblname)) {
				String idxname = rm.getString("indexname");
				String fldname = rm.getString("fieldname");
//				IndexInfo ii = new HashIndexInfo(idxname, tblname, fldname);
				IndexInfo ii = new BTreeIndexInfo(idxname, tblname, fldname);
				result.put(fldname, ii);
			}
		rm.close();
		return result;
	}
}
