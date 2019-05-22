package tinydb.record;

import tinydb.consts.Types;
import java.util.*;

// The schema of a table.
// Contain the information of each fields(attributes) of the table
public class Schema {
	// key: fldname value: fldinfo
	private HashMap<String, Field> fields = new HashMap<String, Field>();

	public Schema() {
	}

	// Add a field to the schema
	public void addField(String fldname, int type) {
		fields.put(fldname, new Field(type));
	}
	
	public void addField(String fldname, int type, int length) {
		fields.put(fldname, new Field(type, length));
	}

	public void addField(String fldname, int type, boolean isNotNull, boolean isPk) {
		fields.put(fldname, new Field(type, isNotNull, isPk));
	}
	
	public void addField(String fldname, int type, int length, boolean isNotNull, boolean isPk) {
		fields.put(fldname, new Field(type, length, isNotNull, isPk));
	}

	public void addIntField(String fldname) {
		addField(fldname, Types.INTEGER);
	}

	public void addLongField(String fldname) {
		addField(fldname, Types.LONG);
	}

	public void addFloatField(String fldname) {
		addField(fldname, Types.FLOAT);
	}

	public void addDoubleField(String fldname) {
		addField(fldname, Types.DOUBLE);
	}

	public void addStringField(String fldname, int length) {
		addField(fldname, Types.STRING, length);
	}

	public void addIntField(String fldname, boolean isNotNull, boolean isPk) {
		addField(fldname, Types.INTEGER, isNotNull, isPk);
	}

	public void addLongField(String fldname, boolean isNotNull, boolean isPk) {
		addField(fldname, Types.LONG, isNotNull, isPk);
	}

	public void addFloatField(String fldname, boolean isNotNull, boolean isPk) {
		addField(fldname, Types.FLOAT, isNotNull, isPk);
	}

	public void addDoubleField(String fldname, boolean isNotNull, boolean isPk) {
		addField(fldname, Types.DOUBLE, isNotNull, isPk);
	}

	public void addStringField(String fldname, int length, boolean isNotNull, boolean isPk) {
		addField(fldname, Types.STRING, length, isNotNull, isPk);
	}
	
	public void add(String fldname, Schema sch) {
		int type = sch.type(fldname);
		int length = sch.length(fldname);
		addField(fldname, type, length);
	}

	public void addAll(Schema sch) {
		if (sch != null)
			fields.putAll(sch.fields);
	}

	public ArrayList<String> fields() {
		return new ArrayList<String> (fields.keySet());
	}

	public boolean hasField(String fldname) {
		return fields().contains(fldname);
	}
	
	// getField
	public Field getField(String fldname) {
		return fields.get(fldname);
	}

	public int type(String fldname) {
		return fields.get(fldname).type;
	}

	public ArrayList<Integer> types() {
		ArrayList<Integer> types = new ArrayList<Integer>();
		for (String fldname : fields())
			types.add(type(fldname));
		return types;
	}
	
	public int length(String fldname) {
		return fields.get(fldname).length;
	}

	public int isNotNull(String fldname) {
		return fields.get(fldname).notNull ? 1 : 0;
	}

	public ArrayList<String> getNotNull() {
		ArrayList<String> l = new ArrayList<String>();
		for (Map.Entry<String, Field> entry : fields.entrySet()) {
			if (entry.getValue().notNull == true)
				l.add(entry.getKey());
		}
		return l;
	}

	public int isPk(String fldname) {
		return fields.get(fldname).isPk ? 1 : 0;
	}

	public String getPk() {
		String pk = "";
		for (Map.Entry<String, Field> entry : fields.entrySet()) {
			if (entry.getValue().isPk == true) {
				pk = entry.getKey();
				break;
			}
		}
		return pk;
	}

	public void setPk(String fldname) {
		fields.get(fldname).isPk = true;
	}
	
	public boolean equals(Schema sch) {
		return fields().equals(sch.fields()) && types().equals(sch.types());
	}

	// Information about a field
	class Field {
		int type, length;
		boolean notNull, isPk;
		
		public Field(int type) {
			this.type = type;
			this.length = 0;
			this.notNull = false;
			this.isPk = false;
		}

		public Field(int type, int length) {
			this.type = type;
			this.length = length;
			this.notNull = false;
			this.isPk = false;
		}
		
		public Field(int type, boolean notNull, boolean isPk) {
			this.type = type;
			this.length = 0;
			this.notNull = notNull;
			this.isPk = isPk;
		}

		public Field(int type, int length, boolean notNull, boolean isPk) {
			this.type = type;
			this.length = length;
			this.notNull = notNull;
			this.isPk = isPk;
		}
	}
}
