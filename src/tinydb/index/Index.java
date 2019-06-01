package tinydb.index;

import tinydb.exec.consts.Constant;
import tinydb.record.RID;

public interface Index {
	public void moveToHead(Constant searchkey);

	public boolean next();

	public RID getDataRid();

	public void insert(Constant dataval, RID datarid);

	public void delete(Constant dataval, RID datarid);

	public void close();
}
