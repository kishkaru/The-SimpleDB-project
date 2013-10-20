package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file; 
    private TupleDesc description;
  /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.description = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        int hashCode = this.file.getAbsoluteFile().hashCode();
        return hashCode;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.description;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws FileNotFoundException, IOException {
        int pageNumber = ((HeapPageId) pid).pageNumber();
        int pageSize = BufferPool.PAGE_SIZE;
        int offset = pageNumber * pageSize;

        RandomAccessFile newFile = new RandomAccessFile(this.file, "r");
        byte[] theArray = new byte[pageSize];

        newFile.seek(offset);
        newFile.read(theArray);
        newFile.close();

        return new HeapPage((HeapPageId) pid, theArray);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageNumber = page.getId().pageNumber();
        int pageSize = BufferPool.PAGE_SIZE;
        int offset = pageNumber * pageSize;

        RandomAccessFile newFile = new RandomAccessFile(this.file, "rw");
        byte[] theArray = page.getPageData();

        newFile.seek(offset);
        newFile.write(theArray);
        newFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long lengthLong = file.length();
        double lengthDouble = (double) lengthLong;
        double numPages = lengthDouble / BufferPool.PAGE_SIZE;
        return (int) Math.ceil(numPages);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pageList = new ArrayList<Page>();

        boolean done = false;
        for(int i=0; i< this.numPages(); i++){
            HeapPage thePage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if(thePage.getNumEmptySlots() > 0){
                thePage.insertTuple(t);
                pageList.add(thePage);
                done = true;
                break;
            }
        }

        if(!done){
            System.out.println("adding new page");
            byte[] newData = HeapPage.createEmptyPageData();

            HeapPageId pid = new HeapPageId(this.getId() ,this.numPages());
            HeapPage newPage = new HeapPage(pid, newData);
            newPage.insertTuple(t);
            newPage.markDirty(true,tid);
            pageList.add(newPage);

            FileOutputStream theFile = new FileOutputStream(file, true);
            theFile.write(newData);
            theFile.close();
        }

        return pageList;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, FileNotFoundException, IOException {

        PageId pid = t.getRecordId().getPageId();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        p.deleteTuple(t);
        p.markDirty(true,tid);

        return p;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) throws FileNotFoundException, IOException, DbException, TransactionAbortedException {
        ArrayList<Page> theList = new ArrayList<Page>();

        for(int i=0; i< this.numPages(); i++){
            Page thePage = Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            theList.add(thePage);
        }

        HeapIterator theIterator = new HeapIterator(theList);

        return theIterator;
    }

}

