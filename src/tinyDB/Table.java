package tinyDB;

public class Table {
	Database 	db;
	String 		tableName;
	int			tableType;

	public Table(Database database, String name, int type) {
        this.db		      	= database;
        this.tableName     	= name;
        this.tableType		= type;
	}
}
