package tinydb.exec.consts;

public class FloatConstant implements Constant {
	private Float val;

	public FloatConstant(float s) {
		val = s;
	}

	public Object value() {
		return val;
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
}
