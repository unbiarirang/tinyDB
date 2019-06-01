package tinydb.exec.expr;

import tinydb.record.Schema;
import tinydb.exec.Exec;
import tinydb.exec.consts.Constant;

// SQL expression
// An expression is constant or fieldname
public interface Expression {
	public boolean isConstant();
	public boolean isFieldName();

	public Constant getConst();
	public String getFieldName();

	// Methods for FieldNameExpressions
	public Constant evaluate(Exec e);	 // Evaluate value from the specified execution
	public Constant evaluateWithTable(Exec e, String lhsTableName);
	public boolean existIn(Schema sch);  // If the field is in the specified schema
	public String getTableName();        // Get name of the table to which the field belongs
}
