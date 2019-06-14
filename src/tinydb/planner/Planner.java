package tinydb.planner;

import static tinydb.consts.Types.DOUBLE;
import static tinydb.consts.Types.FLOAT;
import static tinydb.consts.Types.INTEGER;
import static tinydb.consts.Types.LONG;
import static tinydb.consts.Types.STRING;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.NoSuchElementException;

import tinydb.server.DBManager;
import tinydb.util.BadSyntaxException;
import tinydb.util.NoPermisionException;
import tinydb.util.NotExistsException;
import tinydb.exec.*;
import tinydb.exec.consts.Constant;
import tinydb.exec.consts.FloatConstant;
import tinydb.exec.consts.IntConstant;
import tinydb.exec.consts.LongConstant;
import tinydb.exec.expr.Comparison;
import tinydb.exec.expr.Condition;
import tinydb.exec.expr.ConstantExpression;
import tinydb.parse.*;
import tinydb.plan.Plan;
import tinydb.plan.ProductPlan;
import tinydb.plan.ProjectPlan;
import tinydb.plan.SelectPlan;
import tinydb.plan.TablePlan;
import tinydb.record.*;

public class Planner implements PlannerBase {
	public Planner() {
	}

	public Plan createQueryPlan(String qry) {
		Parser parser = new Parser(qry);
		QueryData data;
		if (qry.contains("join") || qry.contains("JOIN"))
			data = parser.queryJoin();
		else
			data = parser.query();
		return createPlan(data);
	}

	public Plan createPlan(QueryData data) {
		// Step 1: Create a table plan for each mentioned table
		List<Plan> plans = new ArrayList<Plan>();
		List<String> fields = data.isAll() ? new ArrayList<String>() : data.fields();

		for (String tblname : data.tables()) {
			// Check if table name is valid
			if (!DBManager.metadataManager().getTableNames().contains(tblname))
				throw new NotExistsException("Table: " + tblname + " not exists!");

			// Check user permission
//			if (!DBManager.authManager().isAdmin() && !hasPrivilege(tblname, "select"))
//				throw new NoPermisionException("No permission to select on table " + tblname);

			Schema schema = DBManager.metadataManager().getTableInfo(tblname).schema();
			correctDataTypes(schema, data.cond());
			TablePlan tp = new TablePlan(tblname);
			plans.add(tp);

			if (data.isAll())
				fields.addAll(tp.schema().fields());
		}

		// Check if fields are valid
		for (String fldname : fields) {
			boolean isExist = false;
			for (Plan p : plans) {
				if (p.schema().hasField(fldname))
					isExist = true;
			}
			if (!isExist)
				throw new NotExistsException("Field (" + fldname + ") not exists!");
		}

		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);

		// Step 3: Create a selection plan and add the query condition
		p = new SelectPlan(p, data.cond(), data.lhstables(), fields);

		// Step 4: Project on the field names
		p = new ProjectPlan(p, fields);
		return p;
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

	public ArrayList<String> executeShow(String cmd) {
		Parser parser = new Parser(cmd);
		Object obj = parser.showCmd();
		if (obj instanceof ShowTableData) // SHOW TABLE
			return DBManager.showTableFields((ShowTableData) obj);
		else if (obj instanceof ShowDatabaseData) // SHOW DATABASE
			return DBManager.showDatabaseTables((ShowDatabaseData) obj);
		else // SHOW DATABASES
			return DBManager.showDatabases();
	}

	public int executeDelete(DeleteData data) {
		String tblname = data.tableName();
		// Check user permission
//		if (!DBManager.authManager().isAdmin() && (!hasPrivilege(tblname, "*") || !hasPrivilege(tblname, "delete")))
//			throw new NoPermisionException("No permission to delete on table " + tblname);

		Plan p = new TablePlan(tblname);
		p = new SelectPlan(p, data.cond());
		UpdateExec ue = (UpdateExec) p.exec();
		int count = 0;
		while (ue.next()) {
			ue.delete();
			count++;
		}
		ue.close();
		return count;
	}

	public int executeModify(ModifyData data) {
		String tblname = data.tableName();
		// Check user permission
//		if (!DBManager.authManager().isAdmin() && (!hasPrivilege(tblname, "*") || !hasPrivilege(tblname, "update")))
//			throw new NoPermisionException("No permission to update on table " + tblname);

		Schema schema = DBManager.metadataManager().getTableInfo(tblname).schema();
		correctDataTypes(schema, data.cond());

		Plan p = new TablePlan(data.tableName());
		p = new SelectPlan(p, data.cond());
		UpdateExec ue = (UpdateExec) p.exec();
		int count = 0;
		while (ue.next()) {
			Constant val = data.newValue().evaluate(ue);
			correctDataType(p.schema(), val, data.targetField());
			ue.setVal(data.targetField(), val);
			count++;
		}
		ue.close();
		return count;
	}

	public int executeInsert(InsertData data) {
		String tblname = data.tableName();
		// Check user permission
//		if (!DBManager.authManager().isAdmin() && (!hasPrivilege(tblname, "*") || !hasPrivilege(tblname, "insert")))
//			throw new NoPermisionException("No permission to insert on table " + tblname);

		Plan p = new TablePlan(tblname);
		Schema schema = DBManager.metadataManager().getTableInfo(tblname).schema();
		UpdateExec ue = (UpdateExec) p.exec();
		ue.insert();

		Iterator<Constant> vals = data.vals().iterator();
		List<String> fields = data.isAll() ? schema.fields() : data.fields();

		if (schema.getPk() != "" && !fields.contains(schema.getPk()))
			throw new BadSyntaxException("PRIMARY KEY(" + schema.getPk() + ") cannot be null");

		if (!fields.containsAll(schema.getNotNull()))
			throw new BadSyntaxException("NOT NULL field(" + schema.getNotNull() + ") cannot be null");

		for (String fldname : fields) {
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
				try {
					ue.setFloat(fldname, (float) val.value());
				} catch (ClassCastException e) {
					ue.setFloat(fldname, ((Double) val.value()).floatValue());
					val = new FloatConstant(((Double) val.value()).floatValue());
				}
				break;
			case DOUBLE:
			case STRING:
				ue.setVal(fldname, val);
				break;
			}

		}
		ue.close();
		return 1;
	}

	public int executeCreateTable(CreateTableData data) throws Exception {
		String tblname = data.tableName();
		// Check user permission
//		if (!DBManager.authManager().isAdmin() && (!hasPrivilege(tblname, "*") || !hasPrivilege(tblname, "create")))
//			throw new NoPermisionException("No permission to create on table " + tblname);

		Table tb = DBManager.tableManager().createTable(tblname, data.newSchema());
//		Table tb = DBManager.metadataManager().getTableInfo(tblname);
		if (tb.pk() != null) {
			String qry = "create index " + tblname + "pk on " // index name is {tblname}pk
					+ tblname + " (" + tb.pk() + ")";
			DBManager.planner().executeUpdate(qry);
		}
		return 0;
	}

	public int executeCreateIndex(CreateIndexData data) {
		String tblname = data.tableName();
		// Check user permission
//		if (!DBManager.authManager().isAdmin() && (!hasPrivilege(tblname, "*") || !hasPrivilege(tblname, "create")))
//			throw new NoPermisionException("No permission to create index on table " + tblname);

		DBManager.metadataManager().createIndex(data.indexName(), tblname, data.fieldName());
		return 0;
	}

	private boolean hasPrivilege(String tblname, String privilege) {
		String temp = DBManager.authManager().username() + " " + DBManager.metadataManager().dbname() + " " + tblname
				+ " ";
		if (!DBManager.authManager().checkUserPrivilege(temp + "*")
				&& !DBManager.authManager().checkUserPrivilege(temp + privilege))
			return false;
		return true;
	}

	private void correctDataTypes(Schema schema, Condition cond) {
		for (Comparison term : cond.terms()) {
			if (term.isLhsFieldName() && !term.isRhsFieldName()
					&& schema.hasField(term.getLhsFieldName())) {
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
					case FLOAT:
						val = new FloatConstant(((Double) val.value()).floatValue());
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
			case FLOAT:
				return new FloatConstant(((Double) val.value()).floatValue());
			}
		}
		return val;
	}
}
