package tinydb.planner;

import tinydb.record.Schema;
import tinydb.exec.consts.Constant;
import tinydb.exec.expr.Condition;
import tinydb.metadata.IndexInfo;
import tinydb.plan.*;
import tinydb.server.DBManager;

import java.util.List;
import java.util.Map;

// TablePlan + indexes
class TablePlanner {
	private TablePlan plan;
	private Condition cond;
	private List<String> lhstables;
	private List<String> rhsfields;
	private Schema schema;
	private Map<String, IndexInfo> indexes;

	public TablePlanner(String tblname, Condition cond) {
		this.cond = cond;
		plan = new TablePlan(tblname);
		schema = plan.schema();
		indexes = DBManager.metadataManager().getIndexInfo(tblname);
	}
	
	public TablePlanner(String tblname, Condition cond, List<String> lhstables, List<String> rhsfields) {
		this.cond = cond;
		this.lhstables = lhstables;
		this.rhsfields = rhsfields;
		plan = new TablePlan(tblname);
		schema = plan.schema();
		indexes = DBManager.metadataManager().getIndexInfo(tblname);
	}

	public Plan makeSelectPlan() {
		Plan p = makeIndexSelect();
		if (p == null)
			p = plan;
		return addSelectCond(p);
	}

	public Plan makeJoinPlan(Plan current) {
		Schema currsch = current.schema();
		Condition joincond = cond.joinCond(schema, currsch);
		if (joincond == null)
			return null;
		Plan p = makeIndexJoin(current, currsch);
		if (p == null)
			p = makeProductJoin(current, currsch);
		return p;
	}

	public Plan makeProductPlan(Plan current) {
		Plan p = addSelectCond(plan);
		return new ProductPlan(current, p);
	}

	private Plan makeIndexSelect() {
		for (String fldname : indexes.keySet()) {
			Constant val = cond.getFieldValue(fldname);
			if (val != null) {
				IndexInfo ii = indexes.get(fldname);
				//cond.remove(fldname);
				return new IndexSelectPlan(plan, ii, val, cond.getRelation(fldname), cond.isOr());
			}
		}
		return null;
	}

	private Plan makeIndexJoin(Plan current, Schema currsch) {
		for (String fldname : indexes.keySet()) {
			String outerfield = cond.getAnotherFieldName(fldname);
			if (outerfield != null && currsch.hasField(outerfield)) {
				IndexInfo ii = indexes.get(fldname);
				Plan p = new IndexJoinPlan(current, plan, ii, outerfield);
				p = addSelectCond(p);
				return addJoinCond(p, currsch);
			}
		}
		return null;
	}

	private Plan makeProductJoin(Plan current, Schema currsch) {
		Plan p = makeProductPlan(current);
		return addJoinCond(p, currsch);
	}

	private Plan addSelectCond(Plan p) {
		Condition selectpred = cond.selectCond(schema);
		if (selectpred != null)
			return new SelectPlan(p, selectpred, lhstables, rhsfields);
		else
			return p;
	}

	private Plan addJoinCond(Plan p, Schema currsch) {
		Condition joincond = cond.joinCond(currsch, schema);
		if (joincond != null)
			return new SelectPlan(p, joincond, lhstables, rhsfields);
		else
			return p;
	}
	
	public Schema schema() {
		return schema;
	}
}
