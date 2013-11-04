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

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public Page readPage(PageId pid) {
        RandomAccessFile fp;

        try {
            fp = new RandomAccessFile(f, "r");
        } catch(java.io.FileNotFoundException e) {
            throw new IllegalArgumentException();
        }

        try {
            byte[] page = new byte[BufferPool.PAGE_SIZE];
            fp.seek(BufferPool.PAGE_SIZE * pid.pageNumber());
            fp.read(page, 0, BufferPool.PAGE_SIZE);
            fp.close();
            return new HeapPage(
                new HeapPageId(pid.getTableId(), pid.pageNumber()), page);
        } catch(IOException e) {
            throw new IllegalArgumentException();
        }
    }

    public void writePage(Page page) throws IOException {
        int offset = page.getId().pageNumber() * BufferPool.PAGE_SIZE;
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.seek(offset);
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(f.length() / BufferPool.PAGE_SIZE);
    }

    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pageList = new ArrayList<Page>();

        HeapPage page = null;
        for(int i = 0; i < numPages(); i++) {
             HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, 
                     new HeapPageId(getId(), i), Permissions.READ_WRITE);
             if (p.getNumEmptySlots() > 0) {
                 page = p;
                 pageList.add(page);
                 page.insertTuple(t);
                 return pageList;
             }
        }
        
        long initPages = numPages();
        HeapPageId pid = new HeapPageId(getId(), numPages());
        page = new HeapPage(pid, HeapPage.createEmptyPageData());

        FileOutputStream file = new FileOutputStream(f, true);
        file.write(page.getPageData());
        file.close();

        page = (HeapPage) Database.getBufferPool().getPage(
                tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);

        assert numPages() > initPages;

        return pageList;
    }

    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {

        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);

        return page;
    }

    public DbFileIterator iterator(TransactionId tid) {
        return new HeapIterator(tid, numPages(), getId());
    }

}

