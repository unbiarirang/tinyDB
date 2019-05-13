package tinyDB;

import tinyDB.DataFile.*;


public class DBManager {
   
   private static FileManager FileM;
   
   
   public static FileManager getFileM() { 
	   return FileM; 
   }
   public static void init(String dirname) {
      initFileM(dirname);
   }
   public static void initFileM(String dirname) {
      FileM = new FileManager(dirname);
   }
}
