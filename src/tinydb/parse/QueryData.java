package tinydb.parse;

import tinydb.exec.*;
import java.util.*;

// Data for the SQL SELECT statement.
public class QueryData {
   private Collection<String> lhstables;
   private Collection<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   
   public QueryData(Collection<String> lhstables, Collection<String> fields,
		   Collection<String> tables, Predicate pred) {
	  this.lhstables = lhstables;
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
   }
   
	public Collection<String> lhstables() {
		return lhstables;
	}
   
   public Collection<String> fields() {
      return fields;
   }
   
   public Collection<String> tables() {
      return tables;
   }
   
   public Predicate pred() {
      return pred;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
