package tinyDB.Record;

import tinyDB.DataFile.*;

import java.io.IOException;

import com.sun.prism.impl.Disposer.Record;

import tinyDB.DBManager;
import tinyDB.Table;
import static tinyDB.DataFile.Page.BLOCK_SIZE;

public class RecordFile {
   private Table ti;
   private String filename;
   private RecordPage rp;
   private int currentblknum;


   public RecordFile(Table ti) throws IOException {
      this.ti = ti;
      filename = ti.fileName();
      if (getSize(filename) == 0)
    	  appendBlock();
      moveTo(0);
   }
   
   public int getSize(String Filename) throws IOException {
	   return (int) DBManager.getFileM().getFile(Filename).size() / BLOCK_SIZE;
   }

   public void beforeFirst() {
      moveTo(0);
   }
   
   public boolean next() throws IOException {
      while (true) {
         if (rp.next())
            return true;
         if (atLastBlock())
            return false;
         moveTo(currentblknum + 1);
      }
   }
   
   public int getInt(String fldname) {
      return rp.getInt(fldname);
   }

	public long getLong(String fldname) {
		return rp.getLong(fldname);
	}
	
	public float getFloat(String fldname) {
		return rp.getFloat(fldname);
	}
	
	public double getDouble(String fldname) {
		return rp.getDouble(fldname);
	}

   public String getString(String fldname) {
      return rp.getString(fldname);
   }
   
   public void setInt(String fldname, int val) {
      rp.setInt(fldname, val);
   }
   
   public void setLong(String fldname, long val) {
	  rp.setLong(fldname, val);
   }
   
   public void setFloat(String fldname, float val) {
	  rp.setFloat(fldname, val);
   }
   
   public void setDouble(String fldname, double val) {
	  rp.setDouble(fldname, val);
   }
   
   public void setString(String fldname, String val) {
      rp.setString(fldname, val);
   }
   
   public void delete() {
      rp.delete();
   }
   
   public void insert() throws IOException {
      while (!rp.insert()) {
         if (atLastBlock())
            appendBlock();
          moveTo(currentblknum + 1);
      }
   }
   
   public void write() {
	   rp.p.write(rp.getblk());
   }
   
   public RecordPage getRp() {
	   return rp;
   }
   
   public int getCurBlkNum() {
	   return currentblknum;
   }

   private void moveTo(int b) {
      currentblknum = b;
      Block blk = new Block(filename, currentblknum);
      rp = new RecordPage(blk, ti);
   }
   
   private boolean atLastBlock() throws IOException {
      return currentblknum == getSize(filename) - 1;
   }
   private void appendBlock() {
	   Page p = new Page();
	   p.append(filename);
   }
}