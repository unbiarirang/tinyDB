package tinydb.exec.consts;

// Database constant
public interface Constant extends Comparable<Constant> {
	public Object value();
	public boolean equals(Object obj);
	public int compareTo(Constant c);
}
