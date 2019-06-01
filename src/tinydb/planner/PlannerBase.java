package tinydb.planner;

import java.util.ArrayList;

import tinydb.parse.*;
import tinydb.plan.Plan;

public interface PlannerBase {
	public Plan createQueryPlan(String qry);
	
	public Plan createPlan(QueryData data);
	
	public int executeUpdate(String cmd) throws Exception;
	
	public ArrayList<String> executeShow(String cmd);
	
	public int executeDelete(DeleteData data);

	public int executeModify(ModifyData data);

	public int executeInsert(InsertData data);

	public int executeCreateTable(CreateTableData data) throws Exception;

	public int executeCreateIndex(CreateIndexData data);
}
