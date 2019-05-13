package tinyDB.index;

public interface B {
	Object get(Comparable key); 
	void remove(Comparable key); 
	void insert(Comparable key, Object obj);
}
