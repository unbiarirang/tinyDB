package tinydb.index;

import tinydb.exec.consts.Constant;
import tinydb.record.RID;

public interface Index {
	public void moveToHead(Constant searchkey, String relation, boolean isOr) ;

	public boolean next();

	public RID getDataRid();

	public void insert(Constant dataval, RID datarid);

	public void delete(Constant dataval, RID datarid);
	
	public void modify(Constant oldval,Constant newval, RID datarid);

	public void close();
}
