package tinyDB.DataFile;

public class Block{
    private String filename;
    private int blknum;
    
    public Block(String filename, int blknum){
        this.filename = filename;
        this.blknum = blknum;
    }
    public String Filename(){
        return filename;
    }
    public int number(){
        return blknum;
    }
    public boolean equals(Object obj){
        Block blk = (Block) obj;
        return filename.equals(blk.filename) && blknum == blk.blknum;
    }

    public int hashCode(){
        return toString().hashCode();
    }
}