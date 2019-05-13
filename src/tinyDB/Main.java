package tinyDB;

import tinyDB.DataFile.Block;
import tinyDB.DataFile.FileManager;
import tinyDB.DataFile.Page;
import tinyDB.Record.*;
import tinyDB.Table;

import static tinyDB.DataFile.Page.BLOCK_SIZE;
import static tinyDB.DataFile.Page.INT_SIZE;
import static tinyDB.DataFile.Page.STR_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.rmi.registry.*;
import java.util.HashMap;
import java.util.Map;


public class Main {
    public static void main(String[] args) throws IOException {
		Object[] testdata = new Object[2];
		Map<String,Integer> offsets = new HashMap<String,Integer>();
		//模拟student(ID int, NAME varchar(20))的table，并插入数据

		DBManager.init("test");
		int datalen = 0;
		offsets.put("ID", 0);
		datalen =+ INT_SIZE;
		offsets.put("NAME", INT_SIZE);
		datalen =+ STR_SIZE(20);
		Table table = new Table("student",offsets,datalen);
		RecordFile RF = new RecordFile(table);
		testdata[0] = 2016080042;
		testdata[1] = "ss";
		insertdata(testdata,RF);
		testdata[0] = 2016080045;
		testdata[1] = "aa";
		insertdata(testdata,RF);
		testdata[0] = 2016080046;
		testdata[1] = "Sulli";
		insertdata(testdata,RF);
		search("ID",2,RF);
		System.out.println("---------------");
		delete("NAME","aa",RF);
		search("ID",2,RF);
	 }
	public static void insertdata(Object[] obj, RecordFile RF) throws IOException {
		 RF.insert();
		 RF.setInt("ID",(int)obj[0]);
		 RF.setString("NAME", obj[1].toString());
		 RF.write();
	 }
	public static void search(String attr, int val,RecordFile RF) throws IOException {
		RF.beforeFirst();
		while(RF.next()) {
			System.out.println(RF.getInt(attr) + RF.getString("NAME"));
		}
	}
	public static void delete(String attr, Object val,RecordFile RF) throws IOException {
		RF.beforeFirst();
		if(attr == "ID") {
			while(RF.next()) {
				int s = RF.getInt(attr);
				if(s == (int)val) {
					RF.delete();
				}
				RF.write();
			}
		}
		else {
			while(RF.next()) {
				String s = RF.getString(attr);
				if(s.equals(val.toString())) {
					RF.delete();
				}
				RF.write();
			}
		}
	}
}
