package tinydb.plan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import tinydb.exec.*;
import tinydb.parse.*;
import tinydb.record.*;
import tinydb.server.DBManager;

public class Planner {
	public Planner() {
	}

	public Plan createQueryPlan(String qry) {
		Parser parser = new Parser(qry);
		QueryData data;
		if (qry.contains("join"))
			data = parser.queryJoin();
		else
			data = parser.query();
		return createPlan(data);
	}

	public int executeUpdate(String cmd) throws Exception {
		Parser parser = new Parser(cmd);
		Object obj = parser.updateCmd();
		if (obj instanceof InsertData)
			return executeInsert((InsertData) obj);
		else if (obj instanceof DeleteData)
			return executeDelete((DeleteData) obj);
		else if (obj instanceof ModifyData)
			return executeModify((ModifyData) obj);
		else if (obj instanceof CreateTableData)
			return executeCreateTable((CreateTableData) obj);
		else if (obj instanceof DropDatabaseData)
			return DBManager.dropDatabase((DropDatabaseData) obj);
		else if (obj instanceof DropTableData) {
			return DBManager.dropTable((DropTableData) obj);
		} else if (obj instanceof String)
			return DBManager.initDatabase((String) obj);
		else
			return 0;
	}

	public ArrayList<String> executeShow(String cmd) {
		Parser parser = new Parser(cmd);
		Object obj = parser.showCmd();
		if (obj instanceof ShowTablesData)
			return DBManager.showDatabaseTables((ShowTablesData) obj);
		else
			return DBManager.showDatabases();
	}

	public Plan createPlan(QueryData data) {
		// Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (String tblname : data.tables()) {
			plans.add(new TablePlan(tblname));
		}

		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);

		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred(), data.lhstables(), data.fields());

		// Step 4: Project on the field names
		p = new ProjectPlan(p, data.fields());
		return p;
	}

	public int executeDelete(DeleteData data) {
		Plan p = new TablePlan(data.tableName());
		p = new SelectPlan(p, data.pred());
		UpdateExec us = (UpdateExec) p.exec();
		int count = 0;
		while (us.next()) {
			us.delete();
			count++;
		}
		us.close();
		return count;
	}

	public int executeModify(ModifyData data) {
		Plan p = new TablePlan(data.tableName());
		p = new SelectPlan(p, data.pred());
		UpdateExec us = (UpdateExec) p.exec();
		int count = 0;
		while (us.next()) {
			Constant val = data.newValue().evaluate(us);
			us.setVal(data.targetField(), val);
			count++;
		}
		us.close();
		return count;
	}

	public int executeInsert(InsertData data) {
		Plan p = new TablePlan(data.tableName());
		Schema schema = DBManager.metadataManager().getTableInfo(data.tableName()).schema();
		UpdateExec us = (UpdateExec) p.exec();
		us.insert();

		Iterator<Constant> iter = data.vals().iterator();
		List<String> fields = data.fields();

		if (!fields.contains(schema.getPk()))
			throw new BadSyntaxException("PRIMARY KEY(" + schema.getPk() + ") cannot be null");

		if (!fields.containsAll(schema.getNotNull()))
			throw new BadSyntaxException("NOT NULL field(" + schema.getNotNull() + ") cannot be null");

		for (String fldname : fields) {
			Constant val;
			try {
				val = iter.next();
			} catch (NoSuchElementException e) {
				throw new BadSyntaxException("Too few values to insert");
			}

			if (schema.isNotNull(fldname) == 1 && val == null)
				throw new BadSyntaxException("NOT NULL field(" + fldname + ") cannot be null");
			else if (schema.isPk(fldname) == 1 && val == null)
				throw new BadSyntaxException("PRIMARY KEY(" + fldname + ") cannot be null");

			us.setVal(fldname, val);
		}
		us.close();
		return 1;
	}

	public int executeCreateTable(CreateTableData data) throws Exception {
		String tblname = data.tableName();
		DBManager.tableManager().createTable(tblname, data.newSchema());
		Table ti = DBManager.metadataManager().getTableInfo(tblname);
		if (ti.pk() != null) {
			String qry = "create index " + tblname + "pk on " // index name is {tblname}pk
					+ tblname + " (" + ti.pk() + ")";
			DBManager.planner().executeUpdate(qry);
		}
		return 0;
	}
}
