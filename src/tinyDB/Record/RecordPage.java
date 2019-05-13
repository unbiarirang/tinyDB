package tinyDB.Record;

import static tinyDB.DataFile.Page.*;
import tinyDB.Table;
import tinyDB.DataFile.Block;
import tinyDB.DataFile.Page;

public class RecordPage {
   public static final int EMPTY = 0, INUSE = 1;
   
   Page p;
   private Block blk;
   private Table tb;
   private int slotsize;
   private int currentslot = -1;
   
   public RecordPage(Block blk, Table tb) {
      this.blk = blk;
      this.p = new Page();
      this.tb = tb;
      p.read(blk);
      slotsize = tb.recordLength() + INT_SIZE;
  }
   
   public Block getblk() {
	   return blk;
   }

   public boolean next() {
      return searchFor(INUSE);
   }
   
   public int getInt(String fldname) {
      int position = fieldpos(fldname);
      return p.getInt(position);
   }
   
   public long getLong(String fldname) {
	  int position = fieldpos(fldname);
	  return p.getLong(position);
   }
   
   public float getFloat(String fldname) {
	  int position = fieldpos(fldname);
	  return p.getFloat(position);
   }
   
   public double getDouble(String fldname) {
	  int position = fieldpos(fldname);
	  return p.getDouble(position);
   }
   
   public String getString(String fldname) {
      int position = fieldpos(fldname);
      return p.getString(position);
   }
   
   public void setInt(String fldname, int val) {
      int position = fieldpos(fldname);
      p.setInt(position, val);
   }
   
   public void setLong(String fldname, long val) {
	  int position = fieldpos(fldname);
	  p.setLong(position, val);
	}

	public void setFloat(String fldname, float val) {
		int position = fieldpos(fldname);
		p.setFloat(position, val);
	}

	public void setDouble(String fldname, double val) {
		int position = fieldpos(fldname);
		p.setDouble(position, val);
	}

   public void setString(String fldname, String val) {
      int position = fieldpos(fldname);
      p.setString(position, val);
   }
   
   public void delete() {
      int position = currentpos();
      p.setInt(position, EMPTY);
   }
   
   public boolean insert() {
      currentslot = -1;
      boolean found = searchFor(EMPTY);
      if (found) {
         int position = currentpos();
         p.setInt(position, INUSE);
      }
      return found;
   }
   
   public void moveToId(int id) {
      currentslot = id;
   }
   
   public int currentId() {
      return currentslot;
   }
   
   private int currentpos() {
      return currentslot * slotsize;
   }
   
   private int fieldpos(String fldname) {
      int offset = INT_SIZE + tb.offset(fldname);
      return currentpos() + offset;
   }
   
   private boolean isValidSlot() {
      return currentpos() + slotsize <= BLOCK_SIZE;
   }
   
   private boolean searchFor(int flag) {
      currentslot++;
      while (isValidSlot()) {
         int position = currentpos();
         if (p.getInt(position) == flag)
            return true;
         currentslot++;
      }
      return false;
   }
}
