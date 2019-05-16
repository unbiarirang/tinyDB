package tinydb.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static tinydb.file.Page.*;
import tinydb.record.*;

public class Main {
	public static void main(String[] args) throws IOException {
		// Create database
		// SQL: create database test
		String dbname = "test";
		DBManager.initDB(dbname);
		
		// Table schema
		// Table TEST(a int, b long, c float, d double, e string(5))
		String tblname = "test";
		ArrayList<String> fldnames = new ArrayList<String>(Arrays.asList("a", "b", "c", "d", "e"));
		// key: fldname, val: fldtype
		HashMap<String, String> fldtypes = new HashMap<String, String>();
		fldtypes.put("a", "Int");
		fldtypes.put("b", "Long");
		fldtypes.put("c", "Float");
		fldtypes.put("d", "Double");
		fldtypes.put("e", "String");
		// key: fldname, val: fldsize
		HashMap<String, Integer> fldsizes = new HashMap<String, Integer>();
		fldsizes.put("a", INT_SIZE);
		fldsizes.put("b", LONG_SIZE);
		fldsizes.put("c", FLOAT_SIZE);
		fldsizes.put("d", DOUBLE_SIZE);
		fldsizes.put("e", STR_SIZE(5));
		
		// Create table with table schema
		// SQL: create table test(a int, b long, c float, d double, e string(5))
		Table table = new Table(tblname, fldnames, fldtypes, fldsizes);
		
		// Create a RecordManager of the table
		RecordManager rm = new RecordManager(table);

		// Create a data object with five attributes
		Object[] row = new Object[5];
		
		// Insert a row(tuple) of data
		// SQL: insert into test(a, b, c, d, e) values (1, 11111111111, 1.0, 1.0, 'aaaaa')
		row[0] = (Integer) 1;
		row[1] = (Long) 11111111111L;
		row[2] = (Float) (float) 1.0;
		row[3] = (Double) 1.0;
		row[4] = (String) "aaaaa";
		rm.insert(fldnames, new ArrayList<Object>(Arrays.asList(row)));

		// Insert another row
		// SQL: insert into test(a, b, c, d, e) values (2, 222222222222, 2.0, 2.0, null)
		row[0] = (Integer) 2;
		row[1] = (Long) 222222222222L;
		row[2] = (Float) (float) 2.0;
		row[3] = (Double) 2.0;
		row[4] = null;
		rm.insert(fldnames, new ArrayList<Object>(Arrays.asList(row)));

		// Print all rows in the table
		// SQL: select * from test
		System.out.println("SQL: select * from test");
		rm.scanAll();
		
		// Print rows with specified condition
		// SQL: select * from test where a=1
		System.out.println("SQL: select * from test where a=1");
		rm.scan("a", (Integer)1);
		
		// Delete rows with specified condition
		// SQL: delete from test where a=1
		System.out.println("SQL: delete from test where a=1; select * from test");
		rm.delete("a", (Integer)1);
		rm.scanAll();
	}
}
