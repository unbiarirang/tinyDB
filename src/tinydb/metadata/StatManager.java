package tinydb.metadata;

import tinydb.record.*;
import java.util.*;

// Statistics manager
class StatManager {
   private TableManager tm;
   private Map<String,StatInfo> tablestats;
   private int numcalls;

   public StatManager(TableManager tm) {
      this.tm = tm;
      refreshStatistics();
   }

   public synchronized StatInfo getStatInfo(String tblname, Table ti) {
      numcalls++;
      if (numcalls > 100)
         refreshStatistics();
      StatInfo si = tablestats.get(tblname);
      if (si == null) {
         si = calcTableStats(ti);
         tablestats.put(tblname, si);
      }
      return si;
   }
   
   private synchronized void refreshStatistics() {
      tablestats = new HashMap<String,StatInfo>();
      numcalls = 0;
      Table tcatmd = tm.getTable("tblcat");
      RecordManager tcatfile = new RecordManager(tcatmd);
      while(tcatfile.next()) {
         String tblname = tcatfile.getString("tblname");
         Table md = tm.getTable(tblname);
         StatInfo si = calcTableStats(md);
         tablestats.put(tblname, si);
      }
      tcatfile.close();
   }
   
   private synchronized StatInfo calcTableStats(Table ti) {
      int numRecs = 0;
      RecordManager rm = new RecordManager(ti);
      int numblocks = 0;
      while (rm.next()) {
         numRecs++;
         numblocks = rm.currentRid().blockNumber() + 1;
      }
      rm.close();
      return new StatInfo(numblocks, numRecs);
   }
}
