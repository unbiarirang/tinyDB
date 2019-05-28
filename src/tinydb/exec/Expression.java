package tinydb.exec;

import tinydb.record.Schema;

// SQL expression
public interface Expression {
	public boolean isConstant();

	public boolean isFieldName();

	public Constant asConstant();

	public String asFieldName();

	public Constant evaluate(Exec e);

	public boolean appliesTo(Schema sch);
}
