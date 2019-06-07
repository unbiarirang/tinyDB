package tinydb.metadata;

import static tinydb.consts.Types.*;
import static tinydb.file.Page.BLOCK_SIZE;
import tinydb.server.DBManager;
import tinydb.record.*;
import tinydb.index.Index;
import tinydb.index.bptree.*;
import tinydb.index.hash.HashIndex;

public class BTreeIndexInfo implements IndexInfo {
	private String idxname, fldname;
	private Table tb;
	private StatInfo si;

	public BTreeIndexInfo(String idxname, String tblname, String fldname) {
		this.idxname = idxname;
		this.fldname = fldname;
		tb = DBManager.metadataManager().getTableInfo(tblname);
		si = DBManager.metadataManager().getStatInfo(tblname, tb);
	}
	
	public String type() {
		return "BTree";
	}

	public Index open() {
		Schema sch = schema();
		// Create new BPTreeIndex for hash indexing
		return new BPTreeIndex(idxname, sch);
	}

	public int blocksAccessed() {
		Table idxti = new Table("", schema());
		int rpb = BLOCK_SIZE / idxti.recordLength();
		int numblocks = si.recordsOutput() / rpb;
		// Call BPTreeIndex.searchCost for hash indexing
		return BPTreeIndex.searchCost(numblocks, rpb);
	}

	public int recordsOutput() {
		return si.recordsOutput() / si.distinctValues(fldname);
	}
	
	public int distinctValues(String fname) {
		if (fldname.equals(fname))
			return 1;
		else
			return Math.min(si.distinctValues(fldname), recordsOutput());
	}

	public Schema schema() {
		Schema sch = new Schema();
		sch.addIntField("block");
		sch.addIntField("id");
		if (tb.schema().type(fldname) == INTEGER)
			sch.addIntField("dataval");
		else if (tb.schema().type(fldname) == LONG)
			sch.addLongField("dataval");
		else if (tb.schema().type(fldname) == FLOAT)
			sch.addFloatField("dataval");
		else if (tb.schema().type(fldname) == DOUBLE)
			sch.addDoubleField("dataval");
		else {
			int fldlen = tb.schema().length(fldname);
			sch.addStringField("dataval", fldlen);
		}
		return sch;
	}
}
