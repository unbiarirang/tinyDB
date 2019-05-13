package tinyDB.DataFile;

import tinyDB.DBManager;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;


public class Page {

   public static final int BLOCK_SIZE = 50;
   
   public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
   public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
   public static final int FLOAT_SIZE = Float.SIZE / Byte.SIZE;
   public static final int DOUBLE_SIZE = Double.SIZE / Byte.SIZE;
   
   public static final int STR_SIZE(int n) {
      float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
      return INT_SIZE + (n * (int)bytesPerChar);
   }
   
   private ByteBuffer contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
   private FileManager FileManager = DBManager.getFileM();
   
   public Page() {}
   
   public synchronized void read(Block blk) {
      FileManager.read(blk, contents);
   }
   
   public synchronized void write(Block blk) {
      FileManager.write(blk, contents);
   }
   
   public synchronized Block append(String filename) {
      return FileManager.append(filename, contents);
   }
   
   public synchronized int getInt(int offset) {
      contents.position(offset);
      return contents.getInt();
   }
   
   public synchronized void setInt(int offset, int val) {
      contents.position(offset);
      contents.putInt(val);
   }
   
   public synchronized long getLong(int offset) {
	  contents.position(offset);
	  return contents.getLong();
   }

   public synchronized void setLong(int offset, long val) {
	  contents.position(offset);
	  contents.putLong(val);
   }
   
   public synchronized float getFloat(int offset) {
	  contents.position(offset);
	  return contents.getFloat();
   }

   public synchronized void setFloat(int offset, float val) {
	  contents.position(offset);
	  contents.putFloat(val);
   }   
   
   public synchronized double getDouble(int offset) {
	  contents.position(offset);
	  return contents.getDouble();
   }

   public synchronized void setDouble(int offset, double val) {
	  contents.position(offset);
	  contents.putDouble(val);
   }

   public synchronized String getString(int offset) {
      contents.position(offset);
      int len = contents.getInt();
      byte[] byteval = new byte[len];
      contents.get(byteval);
      return new String(byteval);
   }
   
   public synchronized void setString(int offset, String val) {
      contents.position(offset);
      byte[] byteval = val.getBytes();
      contents.putInt(byteval.length);
      contents.put(byteval);
   }
}
