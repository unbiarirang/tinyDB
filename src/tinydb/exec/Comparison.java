package tinydb.exec;

import tinydb.plan.Plan;
import tinydb.record.Schema;

// Comparison between two expressions.
public class Comparison {
   private Expression lhs, rhs;
   private String relation;
   
   public Comparison(Expression lhs, Expression rhs, String r) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.relation = r;
   }
   
   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName),
                         p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      if (lhs.asConstant().equals(rhs.asConstant()))
         return 1;
      else
         return Integer.MAX_VALUE;
   }
   
   public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isConstant())
         return rhs.asConstant();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isConstant())
         return lhs.asConstant();
      else
         return null;
   }

   public String equatesWithField(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }

   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }
   
   public boolean isSatisfied(Exec e) {
      Constant lhsval = lhs.evaluate(e);
      Constant rhsval = rhs.evaluate(e);

      switch (relation) {
      case ">":
    	  return lhsval.compareTo(rhsval) > 0;
      case ">=":
    	  return lhsval.compareTo(rhsval) >= 0;
      case "<":
    	  return lhsval.compareTo(rhsval) < 0;
      case "<=":
    	  return lhsval.compareTo(rhsval) <= 0;
      case "<>":
    	  return lhsval.compareTo(rhsval) != 0;
      }
      
      // case of "="
      return lhsval.equals(rhsval);
   }
   
   public String toString() {
      return lhs.toString() + "=" + rhs.toString();
   }
}
