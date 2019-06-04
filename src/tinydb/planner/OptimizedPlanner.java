package tinydb.planner;

import tinydb.exec.UpdateExec;
import tinydb.exec.consts.Constant;
import tinydb.index.Index;
import tinydb.metadata.IndexInfo;
import tinydb.parse.*;
import tinydb.plan.*;
import tinydb.record.Schema;
import tinydb.record.Table;
import tinydb.server.DBManager;
import tinydb.util.BadSyntaxException;

import java.util.*;

public class OptimizedPlanner implements PlannerBase {
	private Collection<TablePlanner> tableplanners = new ArrayList<TablePlanner>();
	
	public OptimizedPlanner() {}

	public Plan createQueryPlan(String qry) {
		Parser parser = new Parser(qry);
		QueryData data;
		if (qry.contains("join"))
			data = parser.queryJoin();
		else
			data = parser.query();
		return createPlan(data);
	}
	
	public Plan createPlan(QueryData data) {
		Collection<String> fields = data.isAll() ? new ArrayList<String>() : data.fields();

		// Step 1: Create a TablePlanner object for each mentioned table
		for (String tblname : data.tables()) {
			TablePlanner tp = new TablePlanner(tblname, data.cond(), data.lhstables(), fields);
			tableplanners.add(tp);

			if (data.isAll())
				fields.addAll(tp.schema().fields());
		}

		// Step 2: Choose the lowest-size plan to begin the join order
		Plan currentplan = getLowestSelectPlan();

		// Step 3: Repeatedly add a plan to the join order
		while (!tableplanners.isEmpty()) {
			Plan p = getLowestJoinPlan(currentplan);
			if (p != null)
				currentplan = p;
			else // no applicable join
				currentplan = getLowestProductPlan(currentplan);
		}

		// Step 4. Project on the field names and return
		return new ProjectPlan(currentplan, fields);
	}

	private Plan getLowestSelectPlan() {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeSelectPlan();
			if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
				besttp = tp;
				bestplan = plan;
			}
		}
		tableplanners.remove(besttp);
		return bestplan;
	}

	private Plan getLowestJoinPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeJoinPlan(current);
			if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
				besttp = tp;
				bestplan = plan;
			}
		}
		if (bestplan != null)
			tableplanners.remove(besttp);
		return bestplan;
	}

	private Plan getLowestProductPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeProductPlan(current);
			if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
				besttp = tp;
				bestplan = plan;
			}
		}
		tableplanners.remove(besttp);
		return bestplan;
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
		else if (obj instanceof CreateIndexData)
			return executeCreateIndex((CreateIndexData) obj);
		else if (obj instanceof DropDatabaseData)
			return DBManager.dropDatabase((DropDatabaseData) obj);
		else if (obj instanceof DropTableData)
			return DBManager.dropTable((DropTableData) obj);
		else if (obj instanceof String)
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

	// Optimized (delete with index)
	public int executeDelete(DeleteData data) {
		Plan p = new TablePlan(data.tableName());
		p = new SelectPlan(p, data.cond());
		UpdateExec ue = (UpdateExec) p.exec();
		
		Schema sch = p.schema();
		ArrayList<String> fields = sch.fields();
		Map<String, IndexInfo> indexes = DBManager.metadataManager().getIndexInfo(data.tableName());
		IndexInfo ii;
		Index idx;

		int count = 0;
		while (ue.next()) {
			// delete data record
			ue.delete();
			
			// delete index record
			for (String fldname : fields) {
				ii = indexes.get(fldname);
				if (ii != null) {
					idx = ii.open();
					idx.delete(ue.getVal(fldname), ue.getRid());
				}
			}
			count++;
		}
		ue.close();
		return count;
	}

	// Optimized (modify with index)
	public int executeModify(ModifyData data) {
		Plan p = new TablePlan(data.tableName());
		p = new SelectPlan(p, data.cond());
		UpdateExec ue = (UpdateExec) p.exec();
		
		Schema sch = p.schema();
		ArrayList<String> fields = sch.fields();
		Map<String, IndexInfo> indexes = DBManager.metadataManager().getIndexInfo(data.tableName());
		IndexInfo ii;
		Index idx;
		
		int count = 0;
		Constant newval, oldval;
		while (ue.next()) {
			newval = data.newValue().evaluate(ue);
			// modify index record
			for (String fldname : fields) {
				ii = indexes.get(fldname);
				if (ii != null) {
					oldval = ue.getVal(fldname);
					idx = ii.open();
					idx.modify(oldval, newval, ue.getRid());
				}
			}
			// modify data record
			ue.setVal(data.targetField(), newval);
			count++;
		}
		ue.close();
		return count;
	}

	// Optimized (insert with index)
	public int executeInsert(InsertData data) {
		String tblname = data.tableName();
		Plan p = new TablePlan(tblname);
		
		Map<String, IndexInfo> indexes = DBManager.metadataManager().getIndexInfo(tblname);
		Schema schema = DBManager.metadataManager().getTableInfo(data.tableName()).schema();
		UpdateExec ue = (UpdateExec) p.exec();

		Iterator<Constant> vals = data.vals().iterator();
		List<String> fields = data.isAll() ? schema.fields() : data.fields();

		if ((schema.getPk() !=  "") && !fields.contains(schema.getPk()))
			throw new BadSyntaxException("PRIMARY KEY(" + schema.getPk() + ") cannot be null");

		if (!fields.containsAll(schema.getNotNull()))
			throw new BadSyntaxException("Must contain not null fields " + schema.getNotNull());

		ue.insert();
		
		IndexInfo ii;
		Index idx;
		for (String fldname : fields) {
			Constant val;
			try {
				val = vals.next();
			} catch (NoSuchElementException e) {
				throw new BadSyntaxException("Too few values to insert");
			}

			if (schema.isNotNull(fldname) == 1 && val == null)
				throw new BadSyntaxException("NOT NULL field(" + fldname + ") cannot be null");
			else if (schema.isPk(fldname) == 1 && val == null)
				throw new BadSyntaxException("PRIMARY KEY(" + fldname + ") cannot be null");

			ue.setVal(fldname, val);

			ii = indexes.get(fldname);
			if (ii != null) {
				idx = ii.open();
				idx.insert(val, ue.getRid());
			}
		}

		ue.close();
		return 1;
	}

	public int executeCreateTable(CreateTableData data) throws Exception {
		String tblname = data.tableName();
		DBManager.tableManager().createTable(tblname, data.newSchema());
		Table tb = DBManager.metadataManager().getTableInfo(tblname);
		if (tb.pk() != null) {
			String qry = "create index " + tblname + "pk on " // index name is {tblname}pk
					+ tblname + " (" + tb.pk() + ")";
			DBManager.planner().executeUpdate(qry);
		}
		return 0;
	}

	public int executeCreateIndex(CreateIndexData data) {
		DBManager.metadataManager().createIndex(data.indexName(), data.tableName(), data.fieldName());
		return 0;
	}
}
