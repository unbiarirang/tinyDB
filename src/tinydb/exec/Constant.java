package tinydb.exec;

public interface Constant extends Comparable<Constant> {
	public Object asJavaVal();
	public boolean equals(Object obj);
	public int compareTo(Constant c);
	public int hashCode();
	public String toString();
}
