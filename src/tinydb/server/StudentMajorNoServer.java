package tinydb.server;

import tinydb.exec.Exec;
import tinydb.plan.*;
import tinydb.server.DBManager;

public class StudentMajorNoServer {
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
//			Exec s = p.exec();
//			
//			System.out.println("Name\tMajor");
//			while (s.next()) {
//				int sid 	 = s.getInt("sid");
//				String sname = s.getString("sname"); //DBManager stores field names
//				String dname = s.getString("dname"); //in lower case
//				System.out.println(sid + "\t" + sname + "\t" + dname);
//			}
//			s.close();
			
//			// 1. SELECT - null
//			String qry = "select SId, SName from STUDENT";
//			Plan p = DBManager.planner().createQueryPlan(qry);
//			// analogous to the result set
//			Exec s = p.exec();
//			
//			System.out.println("sid\tsname");
//			while (s.next()) {
//				int sid 	 = s.getInt("sid");
//				String sname = s.getString("sname"); //DBManager stores field names
//				System.out.println(sid + "\t" + sname);
//			}
//			s.close();

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
//			Exec s = p.exec();
//			
//			System.out.println("Name\tMajor");
//			while (s.next()) {
//				int sid 	 = s.getInt("sid");
//				String sname = s.getString("sname"); //DBManager stores field names
//				String dname = s.getString("dname"); //in lower case
//				int majorid = s.getInt("majorid");
//				int did = s.getInt("did");
//				System.out.println(sid + "\t" + sname + "\t" + dname + "\t" + majorid + "\t" + did);
//			}
//			s.close();
			
//			// 9. CREATE INDEX
//			String qry9 = "create index TESTIDX on DEPT (did)";
//			DBManager.planner().executeUpdate(qry9);
			
//			// 10. CREATE VIEW
//			String qry10_1 = "create view TESTVIEW as select sname from student";
//			DBManager.planner().executeUpdate(qry10_1);
//			
//			String qry10_2 = "select sname from TESTVIEW";
//			Plan p = DBManager.planner().createQueryPlan(qry10_2);
//			Exec s = p.exec();
//
//			while (s.next()) {
//				String sname = s.getString("sname"); // DBManager stores field names
//				System.out.println(sname + "\t");
//			}
//			s.close();
			
//			// 11. INSERT - null
//			String qry11 = "insert into student(SId, SName, MajorId, GradYear) values "
//						   + "(null, null, null, null)";					// success
//			DBManager.planner().executeUpdate(qry11);
			
			// 12. NOT NULL, PRIMARY KEY
			// drop table first
			String qry = "drop table TEST";
			DBManager.planner().executeUpdate(qry);
			// Two different grammar to create primary key
			String qry12_1 = "create table TEST(a int primary key, b long not null, c float, d double, e string(1))";
			//String qry12_1 = "create table TEST(a int, b long not null, c float, d double, e string(1), primary key (a))";
			DBManager.planner().executeUpdate(qry12_1);
			String qry12_2 = "insert into test(a, b, c, d, e) values (1, 1111111111, 1.0, 1.0, 'c')";
			String qry12_3 = "insert into test(a, b, c, d, e) values (2, 1111111111, null, null, null)";
			DBManager.planner().executeUpdate(qry12_2);
			DBManager.planner().executeUpdate(qry12_3);

			String qry12_4 = "select a, b, c, d ,e from test";
			Plan p = DBManager.planner().createQueryPlan(qry12_4);
			Exec s = p.exec();

			while (s.next()) {
				int a = s.getInt("a");
				long b = s.getLong("b");
				float c = s.getFloat("c");
				double d = s.getDouble("d");
				String e = s.getString("e");
				System.out.println(a + "\t" + b + "\t" + c + "\t" + d + "\t" + e);
			}
			s.close();
			
//			// 13. JOIN with tbname.attrname	
//			String qry13_1 = "drop table JOINTEST1";
//			String qry13_2 = "drop table JOINTEST2";
//			String qry13_3 = "create table JOINTEST1(id int, a int)";
//			String qry13_4 = "create table JOINTEST2(id int, b int)";
//			String qry13_5 = "insert into JOINTEST1(id, a) values (1, 1)";
//			String qry13_6 = "insert into JOINTEST2(id, b) values (1, 2)";
//			String qry13_7 = "insert into JOINTEST2(id, b) values (1, 3)";
//			DBManager.planner().executeUpdate(qry13_1);
//			DBManager.planner().executeUpdate(qry13_2);
//			DBManager.planner().executeUpdate(qry13_3);
//			DBManager.planner().executeUpdate(qry13_4);
//			DBManager.planner().executeUpdate(qry13_5);
//			DBManager.planner().executeUpdate(qry13_6);
//			DBManager.planner().executeUpdate(qry13_7);
//			
//			String qry13_8 = "select JOINTEST1.id, JOINTEST1.a, JOINTEST2.b from JOINTEST1 "
//						   + "join JOINTEST2 on JOINTEST1.id = JOINTEST2.id";
//   			Plan p = DBManager.planner().createQueryPlan(qry13_8);
//			Exec s = p.exec();
//
//			while (s.next()) {
//				int id = s.getInt("jointest1.id");
//				int a = s.getInt("jointest1.a");
//				int b = s.getInt("jointest2.b");
//				System.out.println(id + "\t" + a + "\t" + b);
//			}
//			s.close();
			
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
//			Exec s = p.exec();
//
//			while (s.next()) {
//				int a = s.getInt("a");
//				long b = s.getLong("b");
//				float c = s.getFloat("c");
//				double d = s.getDouble("d");
//				System.out.println(a + "\t" + b + "\t" + c + "\t" + d);
//			}
//			s.close();
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
