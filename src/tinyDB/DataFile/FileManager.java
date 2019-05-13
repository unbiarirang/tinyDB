package tinyDB.DataFile;

import static tinyDB.DataFile.Page.BLOCK_SIZE;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class FileManager{
    private File dbDirectory;
    private boolean isNew;
    private Map<String,FileChannel> openFiles = new HashMap<String,FileChannel>();
    public FileManager(String dbname) {
        String homedir = System.getProperty("user.home");
        dbDirectory = new File(homedir, dbname);
        isNew = !dbDirectory.exists();

        if (isNew && !dbDirectory.mkdir())
            throw new RuntimeException("cannot create " + dbname);
 
        for (String filename : dbDirectory.list())
            if (filename.startsWith("temp"))
            new File(dbDirectory, filename).delete();
    }
    synchronized void read(Block blk, ByteBuffer bb) {
        try {
           bb.clear();
           FileChannel fc = getFile(blk.Filename());
           fc.read(bb, blk.number() * BLOCK_SIZE);
        }
        catch (IOException e) {
           throw new RuntimeException("cannot read block " + blk);
        }
     }
     synchronized void write(Block blk, ByteBuffer bb) {
        try {
           bb.rewind();
           FileChannel fc = getFile(blk.Filename());
           fc.write(bb, blk.number() * BLOCK_SIZE);
        }
        catch (IOException e) {
           throw new RuntimeException("cannot write block" + blk);
        }
     }
     synchronized Block append(String filename, ByteBuffer bb) {
        int newblknum = size(filename);
        Block blk = new Block(filename, newblknum);
        write(blk, bb);
        return blk;
     }
     public synchronized int size(String filename) {
        try {
           FileChannel fc = getFile(filename);
           return (int)(fc.size() / BLOCK_SIZE);
        }
        catch (IOException e) {
           throw new RuntimeException("cannot access " + filename);
        }
     }

     public FileChannel getFile(String filename) throws IOException {
        FileChannel fc = openFiles.get(filename);
        if (fc == null) {
           File dbTable = new File(dbDirectory, filename);
           RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
           fc = f.getChannel();
           openFiles.put(filename, fc);
        }
        return fc;
     }
}