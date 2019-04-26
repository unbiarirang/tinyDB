package tinyDB;
import java.util.ArrayList;

public class Table {
	Database 		db;
	String 			tableName;
	int				tableType;
	
	ArrayList<String>   columnList;
	PersistentStore 	store;
	RowAVL				cache;

	public Table(Database database, String name, int type) {
        this.db		      	= database;
        this.tableName     	= name;
        this.tableType		= type;
        
        this.columnList 		= new ArrayList<String>();
        this.store				= new RowStoreAVL(this);
	}

	public void insert(Object[] data) {
		// Insert into AVLTree
		// ...
	}
}
