package tinyDB;
import java.util.ArrayList;
import java.io.IOException;

public class RowStoreAVL implements PersistentStore {
	public Table 				table;
	public String 				filename;
	public ArrayList<String> 	rowData;

    public RowStoreAVL(Table table) {
    	System.out.println("RowStoreAVL");
    	this.table = table;
    	this.filename = table.db.dbName + ".script";
    	this.rowData = new ArrayList<String>();
    	
    	try {
    		// Read row table data from .script file
			readTableDataDisk();
		} catch (IOException e) {
			// If .script file not exists then create a new .script file
			touchTableDataDisk();
		}
    }
    
    public void touchTableDataDisk() {
    	System.out.println("TouchTableDataDisk");
    	try {
			IOManager.touchFile(this.filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    public ArrayList<String> readTableDataDisk() throws IOException {
    	System.out.println("ReadTableDataDisk");
        this.rowData = IOManager.readLineByLine(this.filename);
        return this.rowData;
    }

    public void writeTableDataDisk(ArrayList<String> newRowData) throws IOException {
    	System.out.println("WriteTableDataDisk");
    	IOManager.appendLineByLine(this.filename, newRowData);
    }

	public CachedObject get(long key) {
		// TODO Auto-generated method stub
		return null;
	}
}