package tinydb.exec;

import tinydb.record.*;

public class SelectExec implements UpdateExec {
   private Exec e;
   private Predicate pred;
   
   public SelectExec(Exec e, Predicate pred) {
      this.e = e;
      this.pred = pred;
   }

   // Exec methods
   
   public void beforeFirst() {
      e.beforeFirst();
   }

   public boolean next() {
      while (e.next())
         if ((!pred.isOr && pred.isSatisfied(e)) || (pred.isOr && pred.isSatisfiedOr(e)))
        	 return true;
      return false;
   }
   
   public void close() {
      e.close();
   }
   
   public Constant getVal(String fldname) {
      return e.getVal(fldname);
   }
   
   public int getInt(String fldname) {
      return e.getInt(fldname);
   }
   
	public long getLong(String fldname) {
		return e.getLong(fldname);
	}
	
	public float getFloat(String fldname) {
		return e.getFloat(fldname);
	}
   
	public double getDouble(String fldname) {
		return e.getDouble(fldname);
	}
   
   public String getString(String fldname) {
      return e.getString(fldname);
   }
   
   public boolean hasField(String fldname) {
      return e.hasField(fldname);
   }
   
   // UpdateExec methods
   
   public void setVal(String fldname, Constant val) {
      UpdateExec us = (UpdateExec) e;
      us.setVal(fldname, val);
   }
   
   public void setInt(String fldname, int val) {
      UpdateExec us = (UpdateExec) e;
      us.setInt(fldname, val);
   }

	public void setLong(String fldname, long val) {
		UpdateExec us = (UpdateExec) e;
		us.setLong(fldname, val);
	}
   
   public void setString(String fldname, String val) {
      UpdateExec us = (UpdateExec) e;
      us.setString(fldname, val);
   }
   
   public void delete() {
      UpdateExec us = (UpdateExec) e;
      us.delete();
   }
   
   public void insert() {
      UpdateExec us = (UpdateExec) e;
      us.insert();
   }
   
   public RID getRid() {
      UpdateExec us = (UpdateExec) e;
      return us.getRid();
   }
   
   public void moveToRid(RID rid) {
      UpdateExec us = (UpdateExec) e;
      us.moveToRid(rid);
   }
}
