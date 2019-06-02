package tinydb.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.net.ServerSocket;
import java.net.Socket;

import tinydb.metadata.TableManager;
import tinydb.parse.*;
import tinydb.record.*;

public class Main {
	public static void main(String[] args) throws Exception {
		// Create or recover database "test"
		// SQL: use database test
		
//		ServerSocket serverSocket = new ServerSocket(8888);
//		Socket c_socket = serverSocket.accept();
//		OutputStream send_data = c_socket.getOutputStream();
//		InputStream recv_data = c_socket.getInputStream();
		
		
		
		String dbname1 = "test1";
		DBManager.initDB(dbname1);
		System.out.println("SQL: use database test1");
		
		// Table schema
		String tblname1 = "table1";
		Schema schema1 = new Schema();
		schema1.addIntField("a");
		schema1.addStringField("b", 5);
		schema1.addLongField("c", true, false);
		schema1.addFloatField("d", true, true);
		String tblname2 = "table2";
		Schema schema2 = new Schema();
		schema2.addIntField("a");

		// Create two tables with schemas
		// SQL: create table table1(a int); create table table2(a int); 
		TableManager tm = DBManager.tableManager();
		Table table1 = tm.createTable(tblname1, schema1);
		Table table2 = tm.createTable(tblname2, schema2);
		
		System.out.println("Database list: \t" + DBManager.showDatabases());
		System.out.println("Table list: \t" + tm.getTableNames());

		// Recover the table from metadata (table/field catalog)
		Table tableRecovered = tm.getTable(tblname1);
		// Make sure that the recovered table is the same as the original table.
		if (!table1.equals(tableRecovered)) 
			throw new Exception("Failed to recover a table accurately!");
		
		// Drop the table
		// SQL: drop table table1
		DBManager.dropTable(new DropTableData(tblname1));
		System.out.println("SQL: drop table test1");
		System.out.println("Database list: \t" + DBManager.showDatabases());
		System.out.println("Table list: \t" + tm.getTableNames());
		
		// Create or recover database "test2"
		// SQL: use database table2
		String dbname2 = "test2";
		DBManager.initDB(dbname2);
		System.out.println("SQL: use database test2");
		System.out.println("Database list: \t" + DBManager.showDatabases());
		System.out.println("Table list: \t" + tm.getTableNames());
		
		// Switch database to test1
		DBManager.initDB(dbname1);
		System.out.println("SQL: use database test1");
		// Drop database
		// SQL: drop database test2
		DBManager.dropDatabase(new DropDatabaseData(dbname2));
		System.out.println("Database list: \t" + DBManager.showDatabases());
		System.out.println("Table list: \t" + tm.getTableNames());
		
		// Create user
		// SQL: CREATE USER user1 PASSWORD password
        String username = "user1";
        String password = "password";
        DBManager.createUser(username , password);
        // Verify user
        System.out.println(DBManager.verifyUser(username, password));
        // Grant privilege
        // SQL: GRANT * ON TABLE tblname1 TO user1
        DBManager.addUserRole(username, tblname1, "*");
        // Revoke privilege
        // SQL: REVOKE * ON TABLE tblname1 FROM user1
        DBManager.removeUserRole(username, tblname1, "*");
        // Delete user
        // SQL: DROP USER user1
        DBManager.deleteUser(username);
        System.out.println(DBManager.verifyUser(username, password));
	}
	
	public void storageTest() throws IOException {
		// Create database
		// SQL: create database test
		String dbname = "test";
		DBManager.initDB(dbname);
		
		// Table schema
		// Table TEST(a int, b long, c float, d double, e string(5))
		String tblname = "test";
		Schema schema = new Schema();
		schema.addIntField("a");
		schema.addLongField("b");
		schema.addFloatField("c");
		schema.addDoubleField("d");
		schema.addStringField("e", 5);
		ArrayList<String> fldnames = new ArrayList<String>(Arrays.asList("a", "b", "c", "d", "e"));
		
		// Create table with table schema
		// SQL: create table test(a int, b long, c float, d double, e string(5))
		TableManager tm = DBManager.tableManager();
		Table table = tm.createTable(tblname, schema);
		
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
		System.out.println("SQL: insert into test(a, b, c, d, e) values (1, 11111111111, 1.0, 1.0, 'aaaaa')");
		rm.insert(fldnames, new ArrayList<Object>(Arrays.asList(row)));

		// Insert another row
		// SQL: insert into test(a, b, c, d, e) values (2, 222222222222, 2.0, 2.0, null)
		row[0] = (Integer) 2;
		row[1] = (Long) 222222222222L;
		row[2] = (Float) (float) 2.0;
		row[3] = (Double) 2.0;
		row[4] = null;
		System.out.println("SQL: insert into test(a, b, c, d, e) values (2, 222222222222, 2.0, 2.0, null)");
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
		rm.deleteAll("a", (Integer)1);
		rm.scanAll();
	}
}
