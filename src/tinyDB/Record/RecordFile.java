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

   /**
    * Constructs an object to manage a file of records.
    * If the file does not exist, it is created.
    * @param ti the table metadata
    * @param tx the transaction
 * @throws IOException 
    */
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
   /**
    * Closes the record file.
    */
   
   /**
    * Positions the current record so that a call to method next
    * will wind up at the first record. 
    */
   public void beforeFirst() {
      moveTo(0);
   }
   
   /**
    *
    * is no next record.
    * @return false if there is no next record.
 * @throws IOException 
    */
   public boolean next() throws IOException {
      while (true) {
         if (rp.next())
            return true;
         if (atLastBlock())
            return false;
         moveTo(currentblknum + 1);
      }
   }
   
   /**
    * Returns the value of the specified field
    * in the current record.
    * @param fldname the name of the field
    * @return the integer value at that field
    */
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

   /**
    * Returns the value of the specified field
    * in the current record.
    * @param fldname the name of the field
    * @return the string value at that field
    */
   public String getString(String fldname) {
      return rp.getString(fldname);
   }
   
   /**
    * Sets the value of the specified field 
    * in the current record.
    * @param fldname the name of the field
    * @param val the new value for the field
    */
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
   
   /**
    * Sets the value of the specified field 
    * in the current record.
    * @param fldname the name of the field
    * @param val the new value for the field
    */
   public void setString(String fldname, String val) {
      rp.setString(fldname, val);
   }
   
   /**
    * Deletes the current record.
    * The client must call next() to move to
    * the next record.
    * Calls to methods on a deleted record 
    * have unspecified behavior.
    */
   public void delete() {
      rp.delete();
   }
   
   /**
    * Inserts a new, blank record somewhere in the file
    * beginning at the current record.
    * If the new record does not fit into an existing block,
    * then a new block is appended to the file.
 * @throws IOException 
    */
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
   
   /**
    * Positions the current record as indicated by the
    * specified RID. 
    * @param rid a record identifier
    */
   //移动到第b个block
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