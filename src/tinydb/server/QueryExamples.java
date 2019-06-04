package tinydb.server;

import java.util.ArrayList;

import tinydb.exec.Exec;
import tinydb.plan.*;
import tinydb.server.DBManager;

public class QueryExamples {
	public static void main(String[] args) {
		try {
			DBManager.initDB("testdb");
			
			Plan p;
			Exec e;

			// 1. SELECT
			String qry1_1 = "drop table TEST";
			// Two different grammar to create primary key
			String qry1_2 = "create table TEST(a int primary key, b long not null, c float)";
			//String qry1_2 = "create table TEST(a int, b long not null, c float, primary key(a))";
			String qry1_3 = "insert into TEST values (1, 111111111, 1.0)";
			String qry1_4 = "insert into TEST(a, b, c) values (2, 222222222, 2.0)";
			String qry1_5 = "insert into TEST(a, b, c) values (3, 333333333, 3.0)";
			String qry1_6 = "insert into TEST(a, b, c) values (4, 444444444, 4.0)";
			String qry1_7 = "insert into TEST(a, b, c) values (5, 555555555, null)";
			DBManager.plannerOpt().executeUpdate(qry1_1);
			DBManager.plannerOpt().executeUpdate(qry1_2);
			DBManager.plannerOpt().executeUpdate(qry1_3);
			DBManager.plannerOpt().executeUpdate(qry1_4);
			DBManager.plannerOpt().executeUpdate(qry1_5);
			DBManager.plannerOpt().executeUpdate(qry1_6);
			DBManager.plannerOpt().executeUpdate(qry1_7);
			
			String qry1_10 = "select * from TEST";
			
			p = DBManager.plannerOpt().createQueryPlan(qry1_10);
			e = p.exec();
			
			System.out.println("a\t b\t c");
			while (e.next()) {
				String a = e.getValToString("a");
				String b = e.getValToString("b");
				String c = e.getValToString("c");
				System.out.println(">>>>>\t" + a + "\t" + b + "\t" + c);
			}
			e.close();


			// 2. CREATE DATABASE dbname
			String qry2 = "create database testdb2";	
			DBManager.plannerOpt().executeUpdate(qry2);

			// 3. USE DATABASE dbname
			String qry3 = "use database testdb";
			DBManager.plannerOpt().executeUpdate(qry3);

			// 4. DROP DATABASE dbname
			String qry4 = "drop database testdb2";
			DBManager.plannerOpt().executeUpdate(qry4);

			// 5. SHOW DATABASE tblname
			String qry5 = "show database testdb";
			ArrayList<String> tableNames = DBManager.plannerOpt().executeShow(qry5);
			System.out.println(tableNames);

			// 6. SHOW DATABASES
			String qry6 = "show databases";
			ArrayList<String> dbNames = DBManager.plannerOpt().executeShow(qry6);
			System.out.println(dbNames);

			// 7. DROP TABLE tblname
			String qry7 = "drop table TEST";
			DBManager.plannerOpt().executeUpdate(qry7);

			// 9. CREATE INDEX
			String qry9 = "create index TESTIDX on JOINTEST1 (a)";
			DBManager.plannerOpt().executeUpdate(qry9);
			
			
			// 13-1. Index SELECT
			String qry13_1 = "drop table JOINTEST1";
			String qry13_2 = "create table JOINTEST1(id1 int, a string(5), primary key(a))";
			String qry13_3 = "insert into JOINTEST1(id1, a) values (1, 'aaaaa')";
			String qry13_4 = "insert into JOINTEST1(id1, a) values (2, 'bbbbb')";
			String qry13_5 = "insert into JOINTEST1(id1, a) values (10, 'ccccc')";
			String qry13_6 = "insert into JOINTEST1(id1, a) values (11, 'ddddd')";
			String qry13_7 = "delete from JOINTEST1 where id1 = 2";
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
			e = p.exec();

			while (e.next()) {
				int id = e.getInt("id1");
				String a = e.getString("a");
				System.out.println(">>>>>\t" + id + "\t" + a);
			}
			e.close();
			
			// 13-2. Index JOIN
			qry13_1 = "drop table JOINTEST1";
			qry13_2 = "drop table JOINTEST2";
			qry13_3 = "create table JOINTEST1(id1 int, a string(5), primary key(id1))";
			qry13_4 = "create table JOINTEST2(id2 int, b string(5), primary key(id2))";
			qry13_5 = "insert into JOINTEST1(id1, a) values (1, 'aaaaa')";
			qry13_6 = "insert into JOINTEST1(id1, a) values (2, 'bbbbb')";
			qry13_7 = "insert into JOINTEST2(id2, b) values (1, 'ccccc')";
			qry13_8 = "insert into JOINTEST2(id2, b) values (2, 'ddddd')";

			DBManager.plannerOpt().executeUpdate(qry13_1);
			DBManager.plannerOpt().executeUpdate(qry13_2);
			DBManager.plannerOpt().executeUpdate(qry13_3);
			DBManager.plannerOpt().executeUpdate(qry13_4);
			DBManager.plannerOpt().executeUpdate(qry13_5);
			DBManager.plannerOpt().executeUpdate(qry13_6);
			DBManager.plannerOpt().executeUpdate(qry13_7);
			DBManager.plannerOpt().executeUpdate(qry13_4);
			DBManager.plannerOpt().executeUpdate(qry13_8);

			qry13_10 = "select id1, a, b from JOINTEST1 join JOINTEST2 on id1 = id2";
			p = DBManager.plannerOpt().createQueryPlan(qry13_10);
			e = p.exec();

			while (e.next()) {
				int id = e.getInt("id1");
				String a = e.getString("a");
				String b = e.getString("b");
				System.out.println(">>>>>\t" + id + "\t" + a + "\t" + b);
			}
			e.close();


			// 13-3. JOIN with tbname.attrname
			qry13_1 = "drop table JOINTEST1";
			qry13_2 = "drop table JOINTEST2";
			qry13_3 = "create table JOINTEST1(id int, a int, primary key(id))";
			qry13_4 = "create table JOINTEST2(id int, b int, primary key(id))";
			qry13_5 = "insert into JOINTEST1(id, a) values (1, 1)";
			qry13_6 = "insert into JOINTEST1(id, a) values (10, 10)";
			qry13_7 = "insert into JOINTEST2(id, b) values (1, 1)";
			String qry13_9 = "insert into JOINTEST2(id, b) values (10, 10)";
			DBManager.plannerOpt().executeUpdate(qry13_1);
			DBManager.plannerOpt().executeUpdate(qry13_2);
			DBManager.plannerOpt().executeUpdate(qry13_3);
			DBManager.plannerOpt().executeUpdate(qry13_4);
			DBManager.plannerOpt().executeUpdate(qry13_5);
			DBManager.plannerOpt().executeUpdate(qry13_6);
			DBManager.plannerOpt().executeUpdate(qry13_7);
			DBManager.plannerOpt().executeUpdate(qry13_9);

			qry13_10 = "select JOINTEST1.id, JOINTEST1.a, JOINTEST2.b from JOINTEST1 "
						   + "join JOINTEST2 on JOINTEST1.id = JOINTEST2.id";
			p = DBManager.plannerOpt().createQueryPlan(qry13_10);
   			e = p.exec();

			while (e.next()) {
				int id = e.getInt("jointest1.id");
				int a = e.getInt("jointest1.a");
				int b = e.getInt("jointest2.b");
				System.out.println(">>>>>\t" + id + "\t" + a + "\t" + b);
			}
			e.close();

			// 13-4. Natural JOIN
			String qry13 = "select id, a, b from JOINTEST1 natural join JOINTEST2";
			p = DBManager.plannerOpt().createQueryPlan(qry13);
   			e = p.exec();

			while (e.next()) {
				int id = e.getInt("id");
				int a = e.getInt("a");
				int b = e.getInt("b");
				System.out.println(">>>>>\t" + id + "\t" + a + "\t" + b);
			}
			e.close();
			


			// 8. multiple JOIN
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
			DBManager.plannerOpt().executeUpdate(qry8_7);;
			DBManager.plannerOpt().executeUpdate(qry8_8);
			DBManager.plannerOpt().executeUpdate(qry8_9);
			DBManager.plannerOpt().executeUpdate(qry8_10);
			DBManager.plannerOpt().executeUpdate(qry8_11);
			DBManager.plannerOpt().executeUpdate(qry8_12);
			
			String qry8 = "select id1, a, b, c "
		        + "from JOINTEST1 "
		        + "join JOINTEST2 on id1 = id2 "
		        + "join JOINTEST3 on id2 = id3 "
		        + "where id1 = 1";
			p = DBManager.plannerOpt().createQueryPlan(qry8);
			e = p.exec();
			
			while (e.next()) {
				int id = e.getInt("id1");
				String a = e.getString("a");
				String b = e.getString("b");
				String c = e.getString("c");
				System.out.println(">>>>>\t" + id + "\t" + a + "\t" + b + "\t" + c);
			}
			e.close();


			// 14. Error test
			String qry14_1 = "drop table TEST";
			String qry14_4 = "drop table TEST2";
			String qry14_2 = "create table TEST(a int primary key, b long not null, c float not null, d double, e string(1))";
			String qryFewValue = "insert into TEST(a, b, c) values (1, 2)";
			String qryTypeNotMatch = "insert into TEST(a, b, c) values (1, 2, 'string')";
			String qryNeedPrimaryKey = "insert into TEST(a, b, c) values (null, 2, 3)";
			String qryNeedPrimaryKey2 = "insert into TEST(b, c) values (2, 3)";
			String qryNeedNotNullValue = "insert into TEST(a, b, c) values (1, null, 3)";
			String qryNeedNotNullValue2 = "insert into TEST(a, c) values (1, 3)";
			String qryTooManyPk1 = "create table TEST2(a int primary key, b int, primary key (b))";
			String qryTooManyPk2 = "create table TEST2(a int, b int, primary key (a, b))";
			
			DBManager.plannerOpt().executeUpdate(qry14_1);
			DBManager.plannerOpt().executeUpdate(qry14_4);
			DBManager.plannerOpt().executeUpdate(qry14_2);
//			DBManager.plannerOpt().executeUpdate(qryFewValue);
//			DBManager.plannerOpt().executeUpdate(qryTypeNotMatch);
//			DBManager.plannerOpt().executeUpdate(qryNeedPrimaryKey2);
//			DBManager.plannerOpt().executeUpdate(qryNeedNotNullValue);
//			DBManager.plannerOpt().executeUpdate(qryNeedNotNullValue2);
//			DBManager.plannerOpt().executeUpdate(qryTooManyPk1);
//			DBManager.plannerOpt().executeUpdate(qryTooManyPk2);
			
			String qry14_3 = "select * from TEST";
			p = DBManager.plannerOpt().createQueryPlan(qry14_3);
			e = p.exec();

			while (e.next())
				System.out.println(e.getValToString("a") + "\t" + e.getValToString("b"));

			e.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
