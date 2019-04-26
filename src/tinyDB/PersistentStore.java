package tinyDB;
import java.io.IOException;
import java.util.ArrayList;

public interface PersistentStore {
	public void touchTableDataDisk();
    public ArrayList<String> readTableDataDisk() throws IOException;
    public void writeTableDataDisk(ArrayList<String> newRowData) throws IOException;
    
    /** get object */
    CachedObject get(long key);
    
}
