package tinydb.exec.consts;

import static tinydb.consts.Types.FLOAT;

public class FloatConstant implements Constant {
	private Float val;
	private boolean isNull = false;

	public FloatConstant(float s) {
		val = s;
	}
	public FloatConstant(float s, boolean isNull) {
		val = s;
		this.isNull = isNull;
	}

	public Object value() {
		return val;
	}
	
	public int type() {
		return FLOAT;
	}
	
	public boolean isNull() {
		return isNull;
	}

	@SuppressWarnings("finally")
	public boolean equals(Object obj) {
		FloatConstant fc = null;
		try {
			fc = (FloatConstant) obj;
		} catch (java.lang.ClassCastException e) {
			fc = new FloatConstant(((Double) ((DoubleConstant) obj).value()).floatValue());
		} finally {
			return fc != null && val.equals(fc.val);
		}
	}

	@SuppressWarnings("finally")
	public int compareTo(Constant c) {
		FloatConstant fc = null;
		try {
			fc = (FloatConstant) c;
		} catch (java.lang.ClassCastException e) {
			fc = new FloatConstant(((Double) ((DoubleConstant) c).value()).floatValue());
		} finally {
			return val.compareTo(fc.val);
		}
	}
	
	public String toString() {
		return val.toString();
	}
}
