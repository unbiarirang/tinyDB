package tinydb.server;

import tinydb.exec.Exec;
import tinydb.plan.*;
import tinydb.server.DBManager;

public class QueryExamples {
	public static void main(String[] args) {
		try {
			// analogous to the driver
			DBManager.initDB("testdb");

//			// 1. SELECT
//			String qry = "select SId, SName, DName "
//		        + "from DEPT, STUDENT "
//		        + "where sid = 1";
//			Plan p = DBManager.planner().createQueryPlan(qry);
//			// analogous to the result set
//			Exec e = p.exec();
//			
//			System.out.println("Name\tMajor");
//			while (e.next()) {
//				int sid 	 = e.getInt("sid");
//				String sname = e.getString("sname"); //DBManager stores field names
//				String dname = e.getString("dname"); //in lower case
//				System.out.println(sid + "\t" + sname + "\t" + dname);
//			}
//			e.close();

//			// 1. SELECT - null
//			String qry = "select SId, SName from STUDENT";
//			Plan p = DBManager.planner().createQueryPlan(qry);
//			// analogous to the result set
//			Exec e = p.exec();
//			
//			System.out.println("sid\tsname");
//			while (e.next()) {
//				int sid 	 = e.getInt("sid");
//				String sname = e.getString("sname"); //DBManager stores field names
//				System.out.println(sid + "\t" + sname);
//			}
//			e.close();

//			// 2. CREATE DATABASE dbname
//			String qry2 = "create database testdb2";	
//			DBManager.planner().executeUpdate(qry2);

//			// 3. USE DATABASE dbname
//			String qry3 = "use database studentdb3";
//			int affected = DBManager.planner().executeUpdate(qry3);

//			// 4. DROP DATABASE dbname
//			String qry4 = "drop database studentdb3";
//			int affected = DBManager.planner().executeUpdate(qry4);

//			// 5. SHOW DATABASE tblname
//			String qry5 = "show database studentdb";
//			String tableNames = DBManager.planner().executeShow(qry5);
//			System.out.print(tableNames);

//			// 6. SHOW DATABASES
//			String qry6 = "show databases";
//			String dbNames = DBManager.planner().executeShow(qry6);
//			System.out.print(dbNames);

//			// 7. DROP TABLE tblname
//			String qry7 = "drop table test";
//			int affected = DBManager.planner().executeUpdate(qry7);
//			System.out.println(affected);

//			// 8. JOIN - multiple join
//			String qry8 = "select SId, SName, DName, majorid, did "
//		        + "from DEPT "          // , => join
//		        + "join STUDENT on MajorId = DId "	  			 // where => on
//		        + "join COURSE on DId = DeptId "
//		        + "where sid <= 5 and sid >= 3";
//			Plan p = DBManager.planner().createQueryPlan(qry8);
//			// analogous to the result set
//			Exec e = p.exec();
//			
//			System.out.println("Name\tMajor");
//			while (e.next()) {
//				int sid 	 = e.getInt("sid");
//				String sname = e.getString("sname"); //DBManager stores field names
//				String dname = e.getString("dname"); //in lower case
//				int majorid = e.getInt("majorid");
//				int did = e.getInt("did");
//				System.out.println(sid + "\t" + sname + "\t" + dname + "\t" + majorid + "\t" + did);
//			}
//			e.close();

//			// 9. CREATE INDEX
//			String qry9 = "create index TESTIDX on DEPT (did)";
//			DBManager.planner().executeUpdate(qry9);

//			// 10. CREATE VIEW
//			String qry10_1 = "create view TESTVIEW as select sname from student";
//			DBManager.planner().executeUpdate(qry10_1);
//			
//			String qry10_2 = "select sname from TESTVIEW";
//			Plan p = DBManager.planner().createQueryPlan(qry10_2);
//			Exec e = p.exec();
//
//			while (e.next()) {
//				String sname = e.getString("sname"); // DBManager stores field names
//				System.out.println(sname + "\t");
//			}
//			e.close();

//			// 11. INSERT - null
//			String qry11 = "insert into student(SId, SName, MajorId, GradYear) values "
//						   + "(null, null, null, null)";					// success
//			DBManager.planner().executeUpdate(qry11);

//			// 12. NOT NULL, PRIMARY KEY
//			// drop table first
//			String qry = "drop table TEST";
//			DBManager.planner().executeUpdate(qry);
//			// Two different grammar to create primary key
//			String qry12_1 = "create table TEST(a int primary key, b long not null, c float, d double, e string(1))";
//			//String qry12_1 = "create table TEST(a int, b long not null, c float, d double, e string(1), primary key (a))";
//			DBManager.planner().executeUpdate(qry12_1);
//			String qry12_2 = "insert into test(a, b, c, d, e) values (1, 1111111111, 1.0, 1.0, 'c')";
//			String qry12_3 = "insert into test(a, b, c, d, e) values (2, 1111111111, null, null, null)";
//			DBManager.planner().executeUpdate(qry12_2);
//			DBManager.planner().executeUpdate(qry12_3);
//
//			String qry12_4 = "select a, b, c, d ,e from test";
//			Plan p = DBManager.planner().createQueryPlan(qry12_4);
//			Exec e = p.exec();
//
//			while (e.next()) {
//				int a = e.getInt("a");
//				long b = e.getLong("b");
//				float c = e.getFloat("c");
//				double d = e.getDouble("d");
//				String e = e.getString("e");
//				System.out.println(a + "\t" + b + "\t" + c + "\t" + d + "\t" + e);
//			}
//			e.close();

//			// 13. JOIN with tbname.attrname
//			Plan p;
//			Exec e;
//			String qry13_1 = "drop table JOINTEST1";
//			String qry13_2 = "drop table JOINTEST2";
//			String qry13_3 = "create table JOINTEST1(id int, a int, primary key(id))";
//			String qry13_4 = "create table JOINTEST2(id int, b int, primary key(id))";
//			String qry13_5 = "insert into JOINTEST1(id, a) values (1, 1)";
//			String qry13_6 = "insert into JOINTEST1(id, a) values (10, 1)";
//			String qry13_7 = "insert into JOINTEST2(id, b) values (1, 2)";
//			String qry13_8 = "insert into JOINTEST2(id, b) values (1, 3)";
//			String qry13_9 = "insert into JOINTEST2(id, b) values (10, 10)";
//			DBManager.planner().executeUpdate(qry13_1);
//			DBManager.planner().executeUpdate(qry13_2);
//			DBManager.planner().executeUpdate(qry13_3);
//			DBManager.planner().executeUpdate(qry13_4);
//			DBManager.planner().executeUpdate(qry13_5);
//			DBManager.planner().executeUpdate(qry13_6);
//			DBManager.planner().executeUpdate(qry13_7);
//			//DBManager.planner().executeUpdate(qry13_8);
//			DBManager.planner().executeUpdate(qry13_9);
//
//			
//			String qry13_10 = "select JOINTEST1.id, JOINTEST1.a, JOINTEST2.b from JOINTEST1 "
//						   + "join JOINTEST2 on JOINTEST1.id = JOINTEST2.id";
//			p = DBManager.planner().createQueryPlan(qry13_10);
//   			e = p.exec();
//
//			while (e.next()) {
//				int id = e.getInt("jointest1.id");
//				int a = e.getInt("jointest1.a");
//				int b = e.getInt("jointest2.b");
//				System.out.println(id + "\t" + a + "\t" + b);
//			}
//			e.close();

			// 13-2. Optimized SELECT
			Plan p;
			Exec e;
			String qry13_1 = "drop table JOINTEST1";
			String qry13_3 = "create table JOINTEST1(id1 int, a string(5), primary key(a))";
			String qry13_5 = "insert into JOINTEST1(id1, a) values (1, 'aaaaa')";
			String qry13_6 = "insert into JOINTEST1(id1, a) values (2, 'bbbbb')";
			String qry13_7 = "insert into JOINTEST1(id1, a) values (10, 'ccccc')";
			String qry13_8 = "delete from JOINTEST1 where id1 = 2";

			DBManager.plannerOpt().executeUpdate(qry13_1);
			DBManager.plannerOpt().executeUpdate(qry13_3);
			DBManager.plannerOpt().executeUpdate(qry13_5);
			DBManager.plannerOpt().executeUpdate(qry13_6);
			DBManager.plannerOpt().executeUpdate(qry13_7);
			DBManager.plannerOpt().executeUpdate(qry13_8);

			String qry13_10 = "select id1, a from JOINTEST1 where a = 'aaaaa'";

			p = DBManager.plannerOpt().createQueryPlan(qry13_10);
			e = p.exec();

			while (e.next()) {
				int id = e.getInt("id1");
				String a = e.getString("a");
				System.out.println(id + "\t" + a);
			}
			e.close();

//			// 14. Error test
//			String qry14_1 = "drop table TEST";
//			String qry14_2 = "create table TEST(a int primary key, b long not null, c float not null, d double, e string(1))";
//			String qryFewValue = "insert into TEST(a, b, c) values (1, 2)";
//			String qryTypeNotMatch = "insert into TEST(a, b, c) values (1, 2, 'string')";
//			String qryNeedPrimaryKey = "insert into TEST(a, b, c) values (null, 2, 3)";
//			String qryNeedPrimaryKey2 = "insert into TEST(b, c) values (2, 3)";
//			String qryNeedNotNullValue = "insert into TEST(a, b, c) values (1, null, 3)";
//			String qryNeedNotNullValue2 = "insert into TEST(a, c) values (1, 3)";
//			String qryTooManyPk = "create table TEST2(a int primary key, b int primary key)";
//			String qryTooManyPk2 = "create table TEST2(a int primary key, b int, primary key (b))";
//			
//			DBManager.planner().executeUpdate(qry14_1);
//			DBManager.planner().executeUpdate(qry14_2);
////			DBManager.planner().executeUpdate(qryFewValue);
////			DBManager.planner().executeUpdate(qryTypeNotMatch);
////			DBManager.planner().executeUpdate(qryNeedPrimaryKey2);
////			DBManager.planner().executeUpdate(qryNeedNotNullValue);
////			DBManager.planner().executeUpdate(qryNeedNotNullValue2);
//			DBManager.planner().executeUpdate(qryTooManyPk2);
//			
//			String qry14_3 = "select a, b, c, d from TEST";
//			Plan p = DBManager.planner().createQueryPlan(qry14_3);
//			Exec e = p.exec();
//
//			while (e.next()) {
//				int a = e.getInt("a");
//				long b = e.getLong("b");
//				float c = e.getFloat("c");
//				double d = e.getDouble("d");
//				System.out.println(a + "\t" + b + "\t" + c + "\t" + d);
//			}
//			e.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
