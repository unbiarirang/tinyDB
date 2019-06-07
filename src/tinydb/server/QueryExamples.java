package tinydb.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tinydb.exec.Exec;
import tinydb.exec.ProjectExec;
import tinydb.plan.*;
import tinydb.server.DBManager;

public class QueryExamples {
	static Plan p;
	public static void main(String[] args) {
		try {
			// analogous to the driver
			DBManager.initDB("testdb");
			
			int[] testcase = { 	  1,	// 0. SELECT
								  0,	// 1. SELECT - avengers examples
								  0,	// 2. CREATE DATABASE dbname
								  0,	// 3. USE DATABASE dbname
								  0,	// 4. DROP DATABASE dbname
								  0,	// 5. SHOW TABLE tblname
								  0,	// 6. SHOW DATABASE dbname
								  0,	// 7. SHOW DATABASES
								  0,	// 8. DROP TABLE tblname
								  0,	// 9. CREATE INDEX
								  0,	// 10. Index SELECT
								  0,	// 11. Index JOIN
								  0,	// 12. JOIN & SELECT with tbname.attrname
								  0,	// 13. Natural JOIN
								  0,	// 14. multiple JOIN
								  0,	// 15. DROP USER
								  0,	// 16. CREATE USER
								  0,	// 17. GRANT PRIVILEGE
								  0,	// 18. REVOKE PRIVIEGE
								  0,	// 19. DELETE
								  0		// 20. Error tests
							 };

			if (testcase[0] == 1) select1();
			if (testcase[1] == 1) select2();
			if (testcase[2] == 1) createDatabase();
			if (testcase[3] == 1) useDatabase();
			if (testcase[4] == 1) dropDatabase();
			if (testcase[5] == 1) showTable();
			if (testcase[6] == 1) showDatabase();
			if (testcase[7] == 1) showDatabases();
			if (testcase[8] == 1) dropTable();
			if (testcase[9] == 1) createIndex();
			if (testcase[10] == 1) indexSelect();
			if (testcase[11] == 1) indexJoin();
			if (testcase[12] == 1) selectWithTblname();
			if (testcase[13] == 1) naturalJoin();
			if (testcase[14] == 1) multipleJoin();
			if (testcase[15] == 1) createUser();
			if (testcase[16] == 1) dropUser();
			if (testcase[17] == 1) grantPrivilege();
			if (testcase[18] == 1) revokePrivilege();
			if (testcase[19] == 1) delete();
			if (testcase[20] == 1) errorTests();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void select1() throws Exception {
		String qry0_1 = "drop table test";
		String qry0_2 = "create table TEST(a int primary key, b long not null, c float)";
		String qry0_3 = "insert into TEST values (1, 111111111, 1.0)";
		String qry0_4 = "insert into TEST values (2, 222222222, 2.0)";
		String qry0_5 = "insert into TEST values (3, 333333333, 3.0)";
		String qry0_6 = "insert into TEST values (4, 444444444, 4.0)";
		String qry0_7 = "insert into TEST values (5, 555555555, null)";

		DBManager.plannerOpt().executeUpdate(qry0_1);
		DBManager.plannerOpt().executeUpdate(qry0_2);
		DBManager.plannerOpt().executeUpdate(qry0_3);
		DBManager.plannerOpt().executeUpdate(qry0_4);
		DBManager.plannerOpt().executeUpdate(qry0_5);
		DBManager.plannerOpt().executeUpdate(qry0_6);
		DBManager.plannerOpt().executeUpdate(qry0_7);

		String qry0_10 = "select * from test where a > 1";
		System.out.println(">>>>>>>>>");
		p = DBManager.plannerOpt().createQueryPlan(qry0_10);
		execPlan(p);
		System.out.println("<<<<<<<<<<");
	}

	private static void select2() throws Exception {
		String qry1_1 = "drop table avengers";
		String qry1_0 = "drop table villain";
		String qry1_2 = "create table avengers" + 
				"	(id			 int not null," + 
				"	 name	     string(32) not null," +
				"	 power	     int not null," + 
				"	 weight      float," + "    height      double," +
				"	 primary key (ID)" + "	);";
		String qry1 = "create table villain" + 
				"	(id			int not null, " + 
				"	 name			string(32) not null, " + 
				"	 power	int not null," + 
				"	 primary key (ID)" + 
				"	);";
		String qry1_3 = "insert into avengers values (10, 'Captain', 50, 78.1, 1.85);";
		String qry1_4 = "insert into avengers values (3, 'Thor', 90, 92.1, 1.89);";
		String qry1_5 = "insert into avengers values (7, 'IronMan', 85, 82.1, 1.76);";
		String qry1_6 = "insert into avengers values (4, 'rocket', 40, 42.1, 0.76);";
		String qry1_7 = "insert into avengers values (5, 'Groot', 10, 182.1, 2.76);";
		String qry1_8 = "INSERT INTO villain VALUES (1, 'Thanos', 100);";
		String qry1_9 = "INSERT INTO villain VALUES (2, 'Red Skull', 40);";
		String qry1_10 = "INSERT INTO villain VALUES (3, 'Hella', 90);";
		String qry1_11 = "INSERT INTO villain VALUES (4, 'monster', 10);";
		DBManager.plannerOpt().executeUpdate(qry1_1);
		DBManager.plannerOpt().executeUpdate(qry1_0);
		DBManager.plannerOpt().executeUpdate(qry1_2);
		DBManager.plannerOpt().executeUpdate(qry1);
		DBManager.plannerOpt().executeUpdate(qry1_3);
		DBManager.plannerOpt().executeUpdate(qry1_4);
		DBManager.plannerOpt().executeUpdate(qry1_5);
		DBManager.plannerOpt().executeUpdate(qry1_6);
		DBManager.plannerOpt().executeUpdate(qry1_7);
		DBManager.plannerOpt().executeUpdate(qry1_8);
		DBManager.plannerOpt().executeUpdate(qry1_9);
		DBManager.plannerOpt().executeUpdate(qry1_10);
		DBManager.plannerOpt().executeUpdate(qry1_11);

		String qry1_12 = "select * from avengers where name = 'Captain';";
		p = DBManager.plannerOpt().createQueryPlan(qry1_12);
		execPlan(p);
		
		String qry1_13 = "select * from villain;";
		p = DBManager.plannerOpt().createQueryPlan(qry1_13);
//		execPlan(p);
		
		String qry = "select avengers.name, villain.name, villain.power " +
				 "from avengers join villain on avengers.power = villain.power " +
				 "where villain.power > 40;";
	
		p = DBManager.plannerOpt().createQueryPlan(qry);
//		execPlan(p);
	}

	private static void createDatabase() throws Exception {
		String qry2 = "create database testdb2";
		DBManager.plannerOpt().executeUpdate(qry2);
	}

	private static void useDatabase() throws Exception {
		String qry3 = "use database testdb";
		DBManager.plannerOpt().executeUpdate(qry3);
	}

	private static void dropDatabase() throws Exception {
		String qry4 = "drop database testdb2";
		DBManager.plannerOpt().executeUpdate(qry4);
	}

	private static void showTable() throws Exception {
		String qry5_0 = "show table avengers;";
		ArrayList<String> tableFields = DBManager.plannerOpt().executeShow(qry5_0);
		System.out.println(">>>>>\t" + tableFields);
	}

	private static void showDatabase() throws Exception {
		String qry5 = "show database testdb";
		ArrayList<String> tableNames = DBManager.plannerOpt().executeShow(qry5);
		System.out.println(">>>>>\t" + tableNames);
	}

	private static void showDatabases() throws Exception {
		String qry6 = "show databases";
		ArrayList<String> dbNames = DBManager.plannerOpt().executeShow(qry6);
		System.out.println(">>>>>\t" + dbNames);
	}

	private static void dropTable() throws Exception {
		String qry7 = "drop table TEST";
		DBManager.plannerOpt().executeUpdate(qry7);
	}

	private static void createIndex() throws Exception {
		String qry9 = "create index TESTIDX on JOINTEST1 (a)";
		DBManager.plannerOpt().executeUpdate(qry9);
	}

	private static void indexSelect() throws Exception {
		String qry13_1 = "drop table JOINTEST1";
		String qry13_2 = "create table JOINTEST1(id1 int, a string(5), primary key(a))";
		String qry13_3 = "insert into JOINTEST1(id1, a) values (1, 'aaaaa')";
		String qry13_4 = "insert into JOINTEST1(id1, a) values (2, 'bbbbb')";
		String qry13_5 = "insert into JOINTEST1(id1, a) values (10, 'ccccc')";
		String qry13_6 = "insert into JOINTEST1(id1, a) values (11, 'ddddd')";
		String qry13_7 = "delete from JOINTEST1 where id1 = 11";
		String qry13_8 = "update JOINTEST1 set a='ddddd' where a='ccccc'";

		DBManager.plannerOpt().executeUpdate(qry13_1);
		DBManager.plannerOpt().executeUpdate(qry13_2);
		DBManager.plannerOpt().executeUpdate(qry13_3);
		DBManager.plannerOpt().executeUpdate(qry13_4);
		DBManager.plannerOpt().executeUpdate(qry13_5);
		DBManager.plannerOpt().executeUpdate(qry13_6);
		DBManager.plannerOpt().executeUpdate(qry13_7);
		DBManager.plannerOpt().executeUpdate(qry13_8);

		String qry13_10 = "select id1, a from JOINTEST1 where a = 'ddddd'";

		p = DBManager.plannerOpt().createQueryPlan(qry13_10);
		execPlan(p);
	}

	private static void indexJoin() throws Exception {
		String qry13_1 = "drop table JOINTEST1";
		String qry13_2 = "drop table JOINTEST2";
		String qry13_3 = "create table JOINTEST1(id1 int, a string(5), primary key(id1))";
		String qry13_4 = "create table JOINTEST2(id2 int, b string(5), primary key(id2))";
		String qry13_5 = "insert into JOINTEST1(id1, a) values (1, 'aaaaa')";
		String qry13_6 = "insert into JOINTEST1(id1, a) values (2, 'bbbbb')";
		String qry13_7 = "insert into JOINTEST2(id2, b) values (1, 'ccccc')";
		String qry13_8 = "insert into JOINTEST2(id2, b) values (2, 'ddddd')";

		DBManager.plannerOpt().executeUpdate(qry13_1);
		DBManager.plannerOpt().executeUpdate(qry13_2);
		DBManager.plannerOpt().executeUpdate(qry13_3);
		DBManager.plannerOpt().executeUpdate(qry13_4);
		DBManager.plannerOpt().executeUpdate(qry13_5);
		DBManager.plannerOpt().executeUpdate(qry13_6);
		DBManager.plannerOpt().executeUpdate(qry13_7);
		DBManager.plannerOpt().executeUpdate(qry13_4);
		DBManager.plannerOpt().executeUpdate(qry13_8);
		String qry13_10 = "select id1, a, b from JOINTEST1 join JOINTEST2 on id1 = id2";
		p = DBManager.plannerOpt().createQueryPlan(qry13_10);
		execPlan(p);
	}

	private static void selectWithTblname() throws Exception {
		String qry13_1 = "drop table JOINTEST1";
		String qry13_2 = "drop table JOINTEST2";
		String qry13_3 = "create table JOINTEST1(id int, a int, primary key(id))";
		String qry13_4 = "create table JOINTEST2(id int, b int, primary key(id))";
		String qry13_5 = "insert into JOINTEST1(id, a) values (1, 1)";
		String qry13_6 = "insert into JOINTEST1(id, a) values (10, 10)";
		String qry13_7 = "insert into JOINTEST2(id, b) values (1, 1111)";
		String qry13_9 = "insert into JOINTEST2(id, b) values (10, 1000)";
		DBManager.plannerOpt().executeUpdate(qry13_1);
		DBManager.plannerOpt().executeUpdate(qry13_2);
		DBManager.plannerOpt().executeUpdate(qry13_3);
		DBManager.plannerOpt().executeUpdate(qry13_4);
		DBManager.plannerOpt().executeUpdate(qry13_5);
		DBManager.plannerOpt().executeUpdate(qry13_6);
		DBManager.plannerOpt().executeUpdate(qry13_7);
		DBManager.plannerOpt().executeUpdate(qry13_9);

		String qry13_10 = "select JOINTEST1.id, JOINTEST1.a, JOINTEST2.b from JOINTEST1 "
				+ "join JOINTEST2 on JOINTEST1.id = JOINTEST2.id";
		p = DBManager.plannerOpt().createQueryPlan(qry13_10);
		execPlan(p);
	}

	private static void naturalJoin() throws Exception {
		String qry13 = "select id, a, b from JOINTEST1 natural join JOINTEST2";
		p = DBManager.plannerOpt().createQueryPlan(qry13);
		execPlan(p);
	}

	private static void multipleJoin() throws Exception {
		String qry8_1 = "drop table JOINTEST1";
		String qry8_2 = "drop table JOINTEST2";
		String qry8_3 = "drop table JOINTEST3";
		String qry8_4 = "create table JOINTEST1(id1 int, a string(5), primary key(id1))";
		String qry8_5 = "create table JOINTEST2(id2 int, b string(5), primary key(id2))";
		String qry8_6 = "create table JOINTEST3(id3 int, c string(5), primary key(id3))";
		String qry8_7 = "insert into JOINTEST1(id1, a) values (1, 'aaaaa')";
		String qry8_8 = "insert into JOINTEST1(id1, a) values (2, 'bbbbb')";
		String qry8_9 = "insert into JOINTEST2(id2, b) values (1, 'ccccc')";
		String qry8_10 = "insert into JOINTEST2(id2, b) values (2, 'ddddd')";
		String qry8_11 = "insert into JOINTEST3(id3, c) values (1, 'eeeee')";
		String qry8_12 = "insert into JOINTEST3(id3, c) values (2, 'fffff')";
		DBManager.plannerOpt().executeUpdate(qry8_1);
		DBManager.plannerOpt().executeUpdate(qry8_2);
		DBManager.plannerOpt().executeUpdate(qry8_3);
		DBManager.plannerOpt().executeUpdate(qry8_4);
		DBManager.plannerOpt().executeUpdate(qry8_5);
		DBManager.plannerOpt().executeUpdate(qry8_6);
		DBManager.plannerOpt().executeUpdate(qry8_7);
		DBManager.plannerOpt().executeUpdate(qry8_8);
		DBManager.plannerOpt().executeUpdate(qry8_9);
		DBManager.plannerOpt().executeUpdate(qry8_10);
		DBManager.plannerOpt().executeUpdate(qry8_11);
		DBManager.plannerOpt().executeUpdate(qry8_12);

		String qry8 = "select id1, a, b, c " + "from JOINTEST1 " + "join JOINTEST2 on id1 = id2 "
				+ "join JOINTEST3 on id2 = id3 where id1 = 1";
		p = DBManager.plannerOpt().createQueryPlan(qry8);
		execPlan(p);
	}


	private static void dropUser() throws Exception {
		String qry = "DROP USER user1";
		DBManager.plannerOpt().executeUpdate(qry);
		boolean isOk = DBManager.verifyUser("user1", "password");
		System.out.println(">>>>>\t" + isOk);
	}
	private static void createUser() throws Exception {
		String qry = "CREATE USER user1 PASSWORD password";
		DBManager.plannerOpt().executeUpdate(qry);
		// login as user1
		boolean isOk = DBManager.verifyUser("user1", "password");
		System.out.println(">>>>>\t" + isOk);
	}

	private static void grantPrivilege() throws Exception {
		String qry = "GRANT select ON TABLE test TO user1";
		DBManager.plannerOpt().executeUpdate(qry);

		select1();
	}

	private static void revokePrivilege() throws Exception {
		String qry = "REVOKE select ON TABLE test FROM user1";
		DBManager.plannerOpt().executeUpdate(qry);

//		select1(); // raise permission error
		// login as admin
		DBManager.verifyUser(null, null);
	}

	private static void delete() throws Exception {
		String qry = "DELETE FROM avengers WHERE name = 'Groot';";
		DBManager.plannerOpt().executeUpdate(qry);

		String qry1 = "select * from avengers";

		p = DBManager.plannerOpt().createQueryPlan(qry1);
		execPlan(p);
	}

	private static void errorTests() throws Exception {
		String qry14_1 = "drop table TEST";
		String qry14_4 = "drop table TEST2";
		String qry14_2 = "create table TEST(a int primary key, b long not null, c float not null)";
		String qry14_3 = "insert into test values (1,1,1);";
		String qryFewValue = "insert into TEST(a, b, c) values (1, 2)";
		String qryTypeNotMatch = "insert into TEST(a, b, c) values (1, 2, 'string')";
		String qryNeedPrimaryKey = "insert into TEST(a, b, c) values (null, 2, 3)";
		String qryNeedPrimaryKey2 = "insert into TEST(b, c) values (2, 3)";
		String qryNeedNotNullValue = "insert into TEST(a, b, c) values (1, null, 3)";
		String qryNeedNotNullValue2 = "insert into TEST(a, c) values (1, 3)";
		String qryTooManyPk1 = "create table TEST2(a int primary key, b int, primary key (b))";
		String qryTooManyPk2 = "create table TEST2(a int, b int, primary key (a, b))";
		String tableNotExists = "insert into NOTTEST values (1);";
		String fieldNotExists = "insert into TEST(a, b, c, d, e) values (1, 1, 1, 1, 1);";
		
		DBManager.plannerOpt().executeUpdate(qry14_1);
		DBManager.plannerOpt().executeUpdate(qry14_4);
		DBManager.plannerOpt().executeUpdate(qry14_2);
		DBManager.plannerOpt().executeUpdate(qry14_3);
//		DBManager.plannerOpt().executeUpdate(qryFewValue);
//		DBManager.plannerOpt().executeUpdate(qryTypeNotMatch);
//		DBManager.plannerOpt().executeUpdate(qryNeedPrimaryKey2);
//		DBManager.plannerOpt().executeUpdate(qryNeedNotNullValue);
//		DBManager.plannerOpt().executeUpdate(qryNeedNotNullValue2);
//		DBManager.plannerOpt().executeUpdate(qryTooManyPk1);
//		DBManager.plannerOpt().executeUpdate(qryTooManyPk2);
//		DBManager.plannerOpt().executeUpdate(tableNotExists);
//		DBManager.plannerOpt().executeUpdate(fieldNotExists);

		String fieldNotExists2 = "select a,b,c,d,e from TEST";
		String tableNotExists2 = "select a from NOTTEST;";
		p = DBManager.plannerOpt().createQueryPlan(tableNotExists2);
		execPlan(p);
	}

	private static void execPlan(Plan p) throws Exception {
		Exec e;
		e = p.exec();
		System.out.println(((ProjectExec) e).tables());
		String test = ((ProjectExec) e).tables();
		System.out.println(((ProjectExec) e).fields());

		while (e.next()) {
			String res = e.getAllVal();
			System.out.println(">>>>>\t" + res);
		}
		e.close();
	}
}
