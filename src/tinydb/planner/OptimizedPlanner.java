package tinydb.planner;

import tinydb.exec.UpdateExec;
import tinydb.exec.consts.Constant;
import tinydb.exec.consts.IntConstant;
import tinydb.exec.consts.LongConstant;
import tinydb.exec.expr.Comparison;
import tinydb.exec.expr.Condition;
import tinydb.exec.expr.ConstantExpression;
import tinydb.index.Index;
import tinydb.metadata.IndexInfo;
import tinydb.parse.*;
import tinydb.plan.*;
import static tinydb.consts.Types.*;
import tinydb.record.Schema;
import tinydb.record.Table;
import tinydb.server.DBManager;
import tinydb.util.BadSyntaxException;
import tinydb.util.NoPermisionException;
import tinydb.util.NotExistsException;

import java.util.*;

public class OptimizedPlanner implements PlannerBase {
	private List<TablePlanner> tableplanners = new ArrayList<TablePlanner>();
	
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
		List<String> fields = data.isAll() ? new ArrayList<String>() : data.fields();

		// Step 1: Create a TablePlanner object for each mentioned table
		for (String tblname : data.tables()) {
			// Check if table name is valid
			if (!DBManager.metadataManager().getTableNames().contains(tblname))
				throw new NotExistsException("Table: " + tblname + " not exists!");
			
			// Check user permission
			if (!DBManager.authManager().isAdmin() && !hasPrivilege(tblname, "select"))
				throw new NoPermisionException("No permission to select on table " + tblname);

			Schema schema = DBManager.metadataManager().getTableInfo(tblname).schema();
			correctDataTypes(schema, data.cond());
			TablePlanner tp = new TablePlanner(tblname, data.cond(), data.lhstables(), fields);
			tableplanners.add(tp);

			if (data.isAll())
				fields.addAll(tp.schema().fields());
		}
		
		// Check if fields are valid
		for (String fldname : fields) {
			boolean isExist= false;
			for (TablePlanner tp : tableplanners) {
				if (tp.schema().hasField(fldname))
					isExist = true;
			}
			if (!isExist)
				throw new NotExistsException("Field (" + fldname + ") not exists!");
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
		else if (obj instanceof CreateUserData)
			return DBManager.createUser((CreateUserData) obj);
		else if (obj instanceof GrantPrivilegeData)
			return DBManager.grantPrivilege((GrantPrivilegeData) obj);
		else if (obj instanceof RevokePrivilegeData)
			return DBManager.revokePrivilege((RevokePrivilegeData) obj);
		else if (obj instanceof DropUserData)
			return DBManager.dropUser((DropUserData) obj);
		else if (obj instanceof DropDatabaseData)
			return DBManager.dropDatabase((DropDatabaseData) obj);
		else if (obj instanceof DropTableData)
			return DBManager.dropTable((DropTableData) obj);
		else if (obj instanceof String)
			return DBManager.initDatabase((String) obj);
		else
			return 0;
	}

	// Execute SQL start with SHOW
	public ArrayList<String> executeShow(String cmd) {
		Parser parser = new Parser(cmd);
		Object obj = parser.showCmd();
		if (obj instanceof ShowTableData)			// SHOW TABLE
			return DBManager.showTableFields((ShowTableData) obj);
		else if (obj instanceof ShowDatabaseData)	// SHOW DATABASE
			return DBManager.showDatabaseTables((ShowDatabaseData) obj);
		else										// SHOW DATABASES
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
			ue.close();
			
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

	// Optimized (update with index)
	public int executeModify(ModifyData data) {
		Schema schema = DBManager.metadataManager().getTableInfo(data.tableName()).schema();
		correctDataTypes(schema, data.cond());

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
			correctDataType(sch, newval, data.targetField());
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
		if (!DBManager.metadataManager().getTableNames().contains(tblname))
			throw new NotExistsException("Table: " + tblname + " not exists!");

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
			if (!schema.hasField(fldname))
				throw new NotExistsException("Field (" + fldname + ") not exists!");

			int fldtype = schema.getField(fldname).type();
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

			
			switch (fldtype) {
			case INTEGER:
				try {
					ue.setInt(fldname, (int) val.value());
				} catch (ClassCastException e) {
					ue.setInt(fldname, ((Double) val.value()).intValue());
					val = new IntConstant(((Double) val.value()).intValue());
				}
				break;
			case LONG:
				try {
					ue.setLong(fldname, (long) val.value());
				} catch (ClassCastException e) {
					ue.setLong(fldname, ((Double) val.value()).longValue());
					val = new LongConstant(((Double) val.value()).longValue());
				}
				break;
			case FLOAT:
			case DOUBLE:
			case STRING:
				ue.setVal(fldname, val);
				break;
			}

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
	
	private boolean hasPrivilege(String tblname, String privilege) {
		String temp = DBManager.authManager().username() + " " + DBManager.metadataManager().dbname()
				+ " " + tblname + " ";
		if (!DBManager.authManager().checkUserPrivilege(temp + "*")
				&& !DBManager.authManager().checkUserPrivilege(temp + privilege))
			return false;
		return true;
	}
	
	private void correctDataTypes(Schema schema, Condition cond) {
		for (Comparison term : cond.terms()) {
			if (term.isLhsFieldName() && !term.isRhsFieldName()) {
				String fldname = term.getLhsFieldName();
				Constant val = term.getFieldValue(fldname);
				int type = schema.type(fldname);
				if (val.type() != type) {
					switch (type) {
					case INTEGER:
						val = new IntConstant(((Double) val.value()).intValue());
						term.modifyRhsValue(new ConstantExpression(val));
						break;
					case LONG:
						val = new LongConstant(((Double) val.value()).longValue());
						term.modifyRhsValue(new ConstantExpression(val));
						break;
					}

				}
			}
		}
	}
	
	private Constant correctDataType(Schema schema, Constant val, String fldname) {
		int type = schema.type(fldname);
		if (val.type() != type) {
			switch (type) {
				case INTEGER:
					return new IntConstant(((Double) val.value()).intValue());
				case LONG:
					return new LongConstant(((Double) val.value()).longValue());
			}
		}
		return val;
	}
}
