package tinydb.exec;

import tinydb.record.RID;

public interface UpdateExec extends Exec {
   public void setVal(String fldname, Constant val);
   public void setInt(String fldname, int val);
   public void setLong(String fldname, long val);
   public void setString(String fldname, String val);

   public void insert();

   public void delete();
   
   public RID  getRid();
   
   public void moveToRid(RID rid);
}
