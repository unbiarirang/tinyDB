package tinydb.parse;

import tinydb.exec.expr.Expression;
import tinydb.exec.expr.Condition;

public class ModifyData {
   private String tblname;
   private String fldname;
   private Expression newval;
   private Condition cond;

   public ModifyData(String tblname, String fldname, Expression newval, Condition cond) {
      this.tblname = tblname;
      this.fldname = fldname;
      this.newval = newval;
      this.cond = cond;
   }
   
   public String tableName() {
      return tblname;
   }
   
   public String targetField() {
      return fldname;
   }
   
   public Expression newValue() {
      return newval;
   }
   
   public Condition cond() {
      return cond;
   }
}