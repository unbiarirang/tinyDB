package tinydb.index.hash;

import tinydb.record.*;
import tinydb.exec.*;
import tinydb.exec.consts.Constant;
import tinydb.exec.consts.StringConstant;
import tinydb.index.Index;

public class HashIndex implements Index {
	public static int NUM_BUCKETS = 100;
	private String idxname;
	private Schema sch;
	private Constant searchkey = null;
	private TableExec te = null;

	public HashIndex(String idxname, Schema sch) {
		this.idxname = idxname;
		this.sch = sch;
	}

	public void moveToHead(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		int bucket = searchkey.value().hashCode() % NUM_BUCKETS;
		String tblname = idxname + bucket;
		Table tb = new Table(tblname, sch);
		te = new TableExec(tb);
		System.out.println("searchkey: " + searchkey.value() + " moveToHead " + tblname);
	}

	public boolean next() {
		while (te.next()) {
			if (te.getVal("dataval").equals(searchkey)) {
				System.out.println("searchkey: " + searchkey.value() + " was found");
				return true;
			}
		}
		return false;
	}

	public RID getDataRid() {
		int blknum = te.getInt("block");
		int id = te.getInt("id");
		return new RID(blknum, id);
	}

	public void insert(Constant val, RID rid) {
		moveToHead(val);
		te.insert();
		te.setInt("block", rid.blockNumber());
		te.setInt("id", rid.id());
		te.setVal("dataval", val);
		close();
	}

	public void delete(Constant val, RID rid) {
		moveToHead(val);
		while(next()) {
			if (getDataRid().equals(rid)) {
				te.delete();
				return;
			}
		}
	}
	
	public void modify(Constant oldval,Constant newval, RID datarid) {
		delete(oldval, datarid);
		insert(newval, datarid);
	}

	public void close() {
		if (te != null)
			te.close();
	}

	public static int searchCost(int numblocks, int rpb){
		return numblocks / HashIndex.NUM_BUCKETS;
	}
}
