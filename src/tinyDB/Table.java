package tinyDB;
import static java.sql.Types.INTEGER;
import static tinyDB.DataFile.Page.*;
import java.util.*;


public class Table {
   private Map<String,Integer> offsets;
   private int recordlen;
   private String tblname;
   
   public Table(String tblname) {
      this.tblname = tblname;

      int pos = 0;

      recordlen = pos;
   }
   
   public Table(String tblname, Map<String,Integer> offsets, int recordlen) {
      this.tblname   = tblname;
      this.offsets   = offsets;
      this.recordlen = recordlen;
   }
   
   public String fileName() {
      return tblname + ".tbl";
   }
   
   public int offset(String fldname) {
      return offsets.get(fldname);
   }

   public int recordLength() {
      return recordlen;
   }
}
