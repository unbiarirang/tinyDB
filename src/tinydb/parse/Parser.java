package tinydb.parse;

import java.util.*;

import tinydb.exec.consts.*;
import tinydb.exec.expr.*;
import tinydb.record.Schema;
import tinydb.util.BadSyntaxException;
import tinydb.util.Tuple;

public class Parser {
	private Lexer lex;

	public Parser(String s) {
		lex = new Lexer(s);
	}

	// Parse conditions, comparisons, expressions, constants, and fields

	public String field() {
		return lex.eatId();
	}

	public Constant constant() {
		if (lex.matchStringConstant()) // string, varchar
			return new StringConstant(lex.eatStringConstant());
		else if (lex.matchIntLongConstant()) // int, long
			return new LongConstant(lex.eatIntLongConstant());
		else if (lex.matchFloatDoubleConstant()) // float, double
			return new DoubleConstant(lex.eatFloatDoubleConstant());
		else // null
		{
			lex.eatKeyword("null");
			return null;
		}
	}

	public Expression expression() {
		if (lex.matchId()) {
			String temp = field();
			if (lex.matchDelim('.')) {
				lex.eatDelim('.');
				return new FieldNameExpression(field(), temp);
			}
			return new FieldNameExpression(temp, "");
		} else
			return new ConstantExpression(constant());
	}

	public Comparison comparison() {
		Expression lhs = expression();
		String relation = "";
		if (lex.matchDelim('<')) {
			lex.eatDelim('<');
			relation += "<";
		}
		if (lex.matchDelim('>')) {
			lex.eatDelim('>');
			relation += ">";
		}
		if (lex.matchDelim('=')) {
			lex.eatDelim('=');
			relation += "=";
		}
		Expression rhs = expression();
		return new Comparison(lhs, rhs, relation);
	}

	public Condition condition() {
		Condition cond = new Condition(comparison());
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			cond.join(condition());
		} else if (lex.matchKeyword("or")) {
			lex.eatKeyword("or");
			cond.join(condition());
			cond.isOr = true;
		}
		return cond;
	}

	public boolean isAll() {
		if (lex.matchDelim('*')) {
			lex.eatDelim('*');
			return true;
		}
		return false;
	}

	// Parse SELECT commands
	public QueryData query() {
		List<String> tableL = null, fieldL = null;

		lex.eatKeyword("select");
		boolean isAll = isAll(); // whether "select *" or not
		if (!isAll) { // query has fields information
			Tuple<List<String>, List<String>> fields = selectList(); // tblname.fldname
			tableL = fields.x;
			fieldL = fields.y;
		}
		lex.eatKeyword("from");

		List<String> tables = tableList();
		Condition cond = new Condition();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			cond = condition();
		}
		return new QueryData(tableL, fieldL, tables, cond, false, isAll);
	}

	// Parse SELECT commands with join
	public QueryData queryJoin() {
		List<String> tableL = null, fieldL = null;
		boolean isNatural = false;

		lex.eatKeyword("select");
		boolean isAll = isAll(); // whether "select *" or not
		if (!isAll) { // query has fields information
			Tuple<List<String>, List<String>> fields = selectList(); // tblname.fldname
			tableL = fields.x;
			fieldL = fields.y;
		}
		lex.eatKeyword("from");

		List<String> tables = tableList();
		Condition cond = new Condition();
		if (lex.matchKeyword("natural")) { // NATURAL JOIN
			isNatural = true;
			lex.eatKeyword("natural");
			lex.eatKeyword("join");
			tables.addAll(tableList());
		} else {
			while (lex.matchKeyword("join")) { // JOIN or multiple JOIN
				lex.eatKeyword("join");
				tables.addAll(tableList());

				lex.eatKeyword("on");
				cond.join(condition());
			}
		}

		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			cond.join(condition());
		}

		return new QueryData(tableL, fieldL, tables, cond, isNatural, isAll);
	}

	// Parse table, field, const list
	private Tuple<List<String>, List<String>> selectList() {
		List<String> tableL = new ArrayList<String>();
		List<String> fieldL = new ArrayList<String>();
		String temp = field();
		if (lex.matchDelim('.')) { // tbname.fldname
			lex.eatDelim('.');
			tableL.add(temp);
			fieldL.add(field());
		} else { // just field name
			tableL.add("");
			fieldL.add(temp);
		}

		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Tuple<List<String>, List<String>> fields = selectList();
			tableL.addAll(fields.x);
			fieldL.addAll(fields.y);
		}
		return new Tuple<List<String>, List<String>>(tableL, fieldL);
	}

	private List<String> tableList() {
		List<String> L = new ArrayList<String>();
		L.add(lex.eatId());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(tableList());
		}
		return L;
	}

	private List<String> fieldList() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(fieldList());
		}
		return L;
	}

	private List<Constant> constList() {
		List<Constant> L = new ArrayList<Constant>();
		L.add(constant());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(constList());
		}
		return L;
	}

	// Parse various update commands

	public Object updateCmd() {
		if (lex.matchKeyword("insert"))
			return insert();
		else if (lex.matchKeyword("delete"))
			return delete();
		else if (lex.matchKeyword("update"))
			return modify();
		else if (lex.matchKeyword("create"))
			return create();
		else if (lex.matchKeyword("use"))
			return use();
//		else if (lex.matchKeyword("show"))
//			return show();
		else
			return drop();
	}

	public Object showCmd() {
		return show();
	}

	private Object create() {
		lex.eatKeyword("create");
		if (lex.matchKeyword("table"))
			return createTable();
		else if (lex.matchKeyword("database"))
			return createUseDatabase();
		else
			return createIndex();
	}

	private Object use() {
		lex.eatKeyword("use");
		return createUseDatabase();
	}

	private Object show() {
		lex.eatKeyword("show");
		if (lex.matchKeyword("database"))
			return showDatabaseTables();
		else {
			showDatabases();
			return null;
		}
	}

	private Object drop() {
		lex.eatKeyword("drop");
		if (lex.matchKeyword("database"))
			return dropDatabase();
		else
			return dropTable();
	}

	// Parse DELETE commands
	public DeleteData delete() {
		lex.eatKeyword("delete");
		lex.eatKeyword("from");
		String tblname = lex.eatId();
		Condition cond = new Condition();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			cond = condition();
		}
		return new DeleteData(tblname, cond);
	}

	// Parse INSERT commands
	public InsertData insert() {
		boolean isAll = false;
		List<String> flds = null;

		lex.eatKeyword("insert");
		lex.eatKeyword("into");
		String tblname = lex.eatId();

		if (lex.matchDelim('(')) { // query has field information
			lex.eatDelim('(');
			flds = fieldList();
			lex.eatDelim(')');
		} else // means all fields
			isAll = true;

		lex.eatKeyword("values");
		lex.eatDelim('(');
		List<Constant> vals = constList();
		lex.eatDelim(')');
		return new InsertData(tblname, flds, vals, isAll);
	}

	// Parse MODIFY commands
	public ModifyData modify() {
		lex.eatKeyword("update");
		String tblname = lex.eatId();
		lex.eatKeyword("set");
		String fldname = field();
		lex.eatDelim('=');
		Expression newval = expression();
		Condition cond = new Condition();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			cond = condition();
		}
		return new ModifyData(tblname, fldname, newval, cond);
	}

	// Parse CREATE TABLE commands
	public CreateTableData createTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		Schema sch = fieldDefs();
		if (lex.matchKeyword("primary"))
			createPk(sch);
		lex.eatDelim(')');
		return new CreateTableData(tblname, sch);
	}

	// Parse table schema (fields definition)

	private Schema fieldDefs() {
		Schema schema = fieldDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}

		return schema;
	}

	private Schema fieldDef() {
		if (lex.matchKeyword("primary"))
			return null;

		String fldname = field();
		return fieldType(fldname);
	}

	private boolean isNotNull() {
		if (lex.matchKeyword("not")) {
			lex.eatKeyword("not");
			lex.eatKeyword("null");
			return true;
		}
		return false;
	}

	private boolean isPk(Schema schema) {
		if (lex.matchKeyword("primary")) {
			lex.eatKeyword("primary");
			lex.eatKeyword("key");
			return true;
		}
		return false;
	}

	private Schema createPk(Schema schema) {
		lex.eatKeyword("primary");
		lex.eatKeyword("key");
		lex.eatDelim('(');
		String fldname = field();
		if (lex.matchDelim(')'))
			lex.eatDelim(')');
		else
			throw new BadSyntaxException("Only one primary key is allowed");

		if (schema.getPk() == null || schema.getPk() == "")
			schema.setPk(fldname);
		else
			throw new BadSyntaxException("Only one primary key is allowed");

		return schema;
	}

	private Schema fieldType(String fldname) {
		Schema schema = new Schema();
		if (lex.matchKeyword("int")) { // INT
			lex.eatKeyword("int");
			schema.addIntField(fldname, isNotNull(), isPk(schema));
		} else if (lex.matchKeyword("long")) { // LONG
			lex.eatKeyword("long");
			schema.addLongField(fldname, isNotNull(), isPk(schema));
		} else if (lex.matchKeyword("float")) { // FLOAT
			lex.eatKeyword("float");
			schema.addFloatField(fldname, isNotNull(), isPk(schema));
		} else if (lex.matchKeyword("double")) { // DOUBLE
			lex.eatKeyword("double");
			schema.addDoubleField(fldname, isNotNull(), isPk(schema));
		} else if (lex.matchKeyword("varchar")) { // VARCHAR
			lex.eatKeyword("varchar");
			lex.eatDelim('(');
			int strLen = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addStringField(fldname, strLen, isNotNull(), isPk(schema));
		} else if (lex.matchKeyword("string")) { // STRING
			lex.eatKeyword("string");
			lex.eatDelim('(');
			int strLen = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addStringField(fldname, strLen, isNotNull(), isPk(schema));
		}
		return schema;
	}

	// Parse CREATE INDEX commands
	public CreateIndexData createIndex() {
		lex.eatKeyword("index");
		String idxname = lex.eatId();
		lex.eatKeyword("on");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		String fldname = field();
		lex.eatDelim(')');
		return new CreateIndexData(idxname, tblname, fldname);
	}

	// Parse CREATE, USE, DROP, SHOW DATABASE commands

	public String createUseDatabase() {
		lex.eatKeyword("database");
		String dbname = lex.eatId();
		return dbname;
	}

	public DropDatabaseData dropDatabase() {
		lex.eatKeyword("database");
		String dbname = lex.eatId();
		return new DropDatabaseData(dbname);
	}

	public DropTableData dropTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		return new DropTableData(tblname);
	}

	public ShowTablesData showDatabaseTables() {
		lex.eatKeyword("database");
		String dbname = lex.eatId();
		return new ShowTablesData(dbname);
	}

	// Parse SHOW DATABASES commands
	public void showDatabases() {
		lex.eatKeyword("databases");
	}
}
