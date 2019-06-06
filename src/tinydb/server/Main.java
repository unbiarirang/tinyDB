package tinydb.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.net.ServerSocket;
import java.net.Socket;

import tinydb.plan.Plan;
import tinydb.exec.Exec;
import tinydb.exec.ProjectExec;
import tinydb.util.Tuple;
import tinydb.util.Utils;

public class Main {
	public static OutputStream output;
	public static InputStream input;
	public static DBManager dm;

	public static void main(String[] args) throws Exception {
		// Create or recover database "test"
		// SQL: use database test
		
		dm = new DBManager();
		ServerSocket serverSocket = new ServerSocket(8888);
		System.out.println("Server is running now");
		Socket c_socket = serverSocket.accept();
		System.out.println("User connected");
		
		OutputStream output_data = c_socket.getOutputStream();
		InputStream input_data = c_socket.getInputStream();
		output = output_data;
		input = input_data;

		while (true) {
			byte[] recvbuffer = new byte[1024];
			input.read(recvbuffer);
			System.out.println(new String(recvbuffer));
			exec(new String(recvbuffer));
		}
	}

	public static void exec(String cmd) throws Exception {
		Plan p;
		Exec e;
		String fstCmd = Utils.parseFirstCmd(cmd);
		System.out.println(cmd);

		fstCmd = fstCmd.toLowerCase();
		switch (fstCmd) {
			case "login":
//				String db = Utils.getDB(cmd);
//				Tuple<String, String> id_pw = Utils.getIDandPW(cmd);
				String[] info = Utils.getInfo(cmd);
				String db = info[0];
				String id = info[1];
				String pw = info[2];
				dm.initDB(db);
				System.out.println("id:" + id);
				System.out.println("pw:" + pw);
				if(id.equals("admin") && pw.equals("admin")) {
					output.write("OK".getBytes());
					break;
				}
				if(DBManager.verifyUser(id, pw) == true) {
					System.out.println("ok");
					output.write("OK".getBytes());
				}
				else {
					output.write("NO".getBytes());
					System.out.println("NO".getBytes());
				}
				break;
				// Just for login, cmd is not a SQL
				// cmd = "login id pw"
				
			// executeUpdate SQL
			case "create":
			case "update":
			case "insert":
			case "delete":
			case "drop":
			case "use":
				DBManager.plannerOpt().executeUpdate(cmd);
				break;
				
			case "select":
				p = DBManager.plannerOpt().createQueryPlan(cmd);
				execPlan(p);
				break;
					// executeShow SQL
			case "show":
				ArrayList<String> names = DBManager.plannerOpt().executeShow(cmd);
				break;
		}
		if(!fstCmd.equals("login") && !fstCmd.equals("select"))
			output.write("completed".getBytes());
	}
	private static void execPlan(Plan p) throws Exception {
		Exec e;
		String contents = "select";
		e = p.exec();
		String tables = ((ProjectExec) e).tables();
		String fields = ((ProjectExec) e).fields();
		contents = contents + "\n" + tables + "\n" + fields + "\n";
		
		while (e.next()) {
			String res = e.getAllVal();
			contents = contents + res + "\n";
			System.out.println(">>>>>\t" + res);
		}
		output.write(contents.getBytes());
		e.close();
	}
}

//		String dbname1 = "test1";
//		DBManager.initDB(dbname1);
//		System.out.println("SQL: use database test1");
//		
//		// Table schema
//		String tblname1 = "table1";
//		Schema schema1 = new Schema();
//		schema1.addIntField("a");
//		schema1.addStringField("b", 5);
//		schema1.addLongField("c", true, false);
//		schema1.addFloatField("d", true, true);
//		String tblname2 = "table2";
//		Schema schema2 = new Schema();
//		schema2.addIntField("a");
//
//		// Create two tables with schemas
//		// SQL: create table table1(a int); create table table2(a int); 
//		TableManager tm = DBManager.tableManager();
//		Table table1 = tm.createTable(tblname1, schema1);
//		Table table2 = tm.createTable(tblname2, schema2);
//		
//		System.out.println("Database list: \t" + DBManager.showDatabases());
//		System.out.println("Table list: \t" + tm.getTableNames());
//
//		// Recover the table from metadata (table/field catalog)
//		Table tableRecovered = tm.getTable(tblname1);
//		// Make sure that the recovered table is the same as the original table.
//		if (!table1.equals(tableRecovered)) 
//			throw new Exception("Failed to recover a table accurately!");
//		
//		// Drop the table
//		// SQL: drop table table1
//		DBManager.dropTable(new DropTableData(tblname1));
//		System.out.println("SQL: drop table test1");
//		System.out.println("Database list: \t" + DBManager.showDatabases());
//		System.out.println("Table list: \t" + tm.getTableNames());
//		
//		// Create or recover database "test2"
//		// SQL: use database table2
//		String dbname2 = "test2";
//		DBManager.initDB(dbname2);
//		System.out.println("SQL: use database test2");
//		System.out.println("Database list: \t" + DBManager.showDatabases());
//		System.out.println("Table list: \t" + tm.getTableNames());
//		
//		// Switch database to test1
//		DBManager.initDB(dbname1);
//		System.out.println("SQL: use database test1");
//		// Drop database
//		// SQL: drop database test2
//		DBManager.dropDatabase(new DropDatabaseData(dbname2));
//		System.out.println("Database list: \t" + DBManager.showDatabases());
//		System.out.println("Table list: \t" + tm.getTableNames());
//		
//		// Create user
//		// SQL: CREATE USER user1 PASSWORD password
//        String username = "user1";
//        String password = "password";
//        DBManager.createUser(username , password);
//        // Verify user
//        System.out.println(DBManager.verifyUser(username, password));
//        // Grant privilege
//        // SQL: GRANT * ON TABLE tblname1 TO user1
//        DBManager.addUserRole(username, tblname1, "*");
//        // Revoke privilege
//        // SQL: REVOKE * ON TABLE tblname1 FROM user1
//        DBManager.removeUserRole(username, tblname1, "*");
//        // Delete user
//        // SQL: DROP USER user1
//        DBManager.deleteUser(username);
//        System.out.println(DBManager.verifyUser(username, password));
//	}
//	
//	public void storageTest() throws IOException {
//		// Create database
//		// SQL: create database test
//		String dbname = "test";
//		DBManager.initDB(dbname);
//		
//		// Table schema
//		// Table TEST(a int, b long, c float, d double, e string(5))
//		String tblname = "test";
//		Schema schema = new Schema();
//		schema.addIntField("a");
//		schema.addLongField("b");
//		schema.addFloatField("c");
//		schema.addDoubleField("d");
//		schema.addStringField("e", 5);
//		ArrayList<String> fldnames = new ArrayList<String>(Arrays.asList("a", "b", "c", "d", "e"));
//		
//		// Create table with table schema
//		// SQL: create table test(a int, b long, c float, d double, e string(5))
//		TableManager tm = DBManager.tableManager();
//		Table table = tm.createTable(tblname, schema);
//		
//		// Create a RecordManager of the table
//		RecordManager rm = new RecordManager(table);
//
//		// Create a data object with five attributes
//		Object[] row = new Object[5];
//		
//		// Insert a row(tuple) of data
//		// SQL: insert into test(a, b, c, d, e) values (1, 11111111111, 1.0, 1.0, 'aaaaa')
//		row[0] = (Integer) 1;
//		row[1] = (Long) 11111111111L;
//		row[2] = (Float) (float) 1.0;
//		row[3] = (Double) 1.0;
//		row[4] = (String) "aaaaa";
//		System.out.println("SQL: insert into test(a, b, c, d, e) values (1, 11111111111, 1.0, 1.0, 'aaaaa')");
//		rm.insert(fldnames, new ArrayList<Object>(Arrays.asList(row)));
//
//		// Insert another row
//		// SQL: insert into test(a, b, c, d, e) values (2, 222222222222, 2.0, 2.0, null)
//		row[0] = (Integer) 2;
//		row[1] = (Long) 222222222222L;
//		row[2] = (Float) (float) 2.0;
//		row[3] = (Double) 2.0;
//		row[4] = null;
//		System.out.println("SQL: insert into test(a, b, c, d, e) values (2, 222222222222, 2.0, 2.0, null)");
//		rm.insert(fldnames, new ArrayList<Object>(Arrays.asList(row)));
//
//		// Print all rows in the table
//		// SQL: select * from test
//		System.out.println("SQL: select * from test");
//		rm.scanAll();
//		
//		// Print rows with specified condition
//		// SQL: select * from test where a=1
//		System.out.println("SQL: select * from test where a=1");
//		rm.scan("a", (Integer)1);
//		
//		// Delete rows with specified condition
//		// SQL: delete from test where a=1
//		System.out.println("SQL: delete from test where a=1; select * from test");
//		rm.deleteAll("a", (Integer)1);
//		rm.scanAll();
//	}
// }
