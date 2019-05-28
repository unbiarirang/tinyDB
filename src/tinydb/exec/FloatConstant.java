package tinydb.exec;

public class FloatConstant implements Constant {
   private Float val;
   
   public FloatConstant(float s) {
      val = s;
   }

   public Object asJavaVal() {
      return val;
   }
   
	public boolean equals(Object obj) {
		FloatConstant fc = null;
		try {
			fc = (FloatConstant) obj;
		} catch (java.lang.ClassCastException e) {
			fc = new FloatConstant(((Double) ((DoubleConstant) obj).asJavaVal()).floatValue());
		} finally {
			return fc != null && val.equals(fc.val);
		}
	}
   
	public int compareTo(Constant c) {
		FloatConstant fc = null;
		try {
			fc = (FloatConstant) c;
		} catch (java.lang.ClassCastException e) {
			fc = new FloatConstant(((Double) ((DoubleConstant) c).asJavaVal()).floatValue());
		} finally {
			return val.compareTo(fc.val);
		}
	}
   
   public int hashCode() {
      return val.hashCode();
   }
   
   public String toString() {
      return val.toString();
   }
}
