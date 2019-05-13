package tinyDB.index;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public class BplusTree implements B, Serializable {

	/** 根节点 */
	protected Node root;
	/** 阶数,M值 */
	protected int degree;
	/** 叶子节点的链表头 */
	protected Node head;
	
	protected boolean isPK;
		
	protected String name;
	
	
	public Node getHead() {
		return head;
	}

	public void setHead(Node head) {
		this.head = head;
	}

	public Node getRoot() {
		return root;
	}

	public void setRoot(Node root) {
		this.root = root;
	}

	public int getDegree() {
		return degree;
	}

	public void setdegree(int degree) {
		this.degree = degree;
	}
	
	public Object get(Comparable key) {
		return root.get(key);
	}

	public void remove(Comparable key) {
		try {
			root.remove(key, this);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void insert(Comparable key, Object obj) {		
		try {
			root.insert(key, obj, this);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public BplusTree(int degree) {
		if (degree < 3) {
			System.out.print("Degree must be greater than 2");
			System.exit(0);
		}
		this.degree = degree;
		this.root = new Node(true, true, this);
		this.head = root;
	}
}
