package tinydb.exec;

public class IntConstant implements Constant {
   private Integer val;

   public IntConstant(int n) {
      val = new Integer(n);
   }
   
   public Object asJavaVal() {
      return val;
   }
   
   public boolean equals(Object obj) {
	  IntConstant ic = null;
	  try {
		  ic = (IntConstant) obj;
	  } catch (java.lang.ClassCastException e) {
		  ic = new IntConstant (((Long)((LongConstant) obj).asJavaVal()).intValue());
	  } finally {
		  return ic != null && val.equals(ic.val);
	  }
   }
   
	public int compareTo(Constant c) {
		IntConstant ic = null;
		try {
			ic = (IntConstant) c;
		} catch (java.lang.ClassCastException e) {
			ic = new IntConstant(((Long) ((LongConstant) c).asJavaVal()).intValue());
		} finally {
			return val.compareTo(ic.val);
		}
	}
   
   public int hashCode() {
      return val.hashCode();
   }
   
   public String toString() {
      return val.toString();
   }
}
