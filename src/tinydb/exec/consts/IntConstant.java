package tinydb.exec.consts;

import static tinydb.consts.Types.INTEGER;

public class IntConstant implements Constant {
	private Integer val;
	private boolean isNull = false;

	public IntConstant(int n) {
		val = new Integer(n);
	}
	public IntConstant(int n, boolean isNull) {
		val = new Integer(n);
		this.isNull = isNull;
	}

	public Object value() {
		return val;
	}
	
	public int type() {
		return INTEGER;
	}
	
	public boolean isNull() {
		return isNull;
	}

	@SuppressWarnings("finally")
	public boolean equals(Object obj) {
		IntConstant ic = null;
		try {
			ic = (IntConstant) obj;
		} catch (java.lang.ClassCastException e) {
			ic = new IntConstant(((Long) ((LongConstant) obj).value()).intValue());
		} finally {
			return ic != null && val.equals(ic.val);
		}
	}

	@SuppressWarnings("finally")
	public int compareTo(Constant c) {
		IntConstant ic = null;
		try {
			ic = (IntConstant) c;
		} catch (java.lang.ClassCastException e) {
			ic = new IntConstant(((Double) ((DoubleConstant) c).value()).intValue());
		} finally {
			return val.compareTo(ic.val);
		}
	}

	public String toString() {
		return val.toString();
	}
}
