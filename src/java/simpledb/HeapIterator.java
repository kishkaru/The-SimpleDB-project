package simpledb; 
import java.io.IOException;
import java.util.*;

public class HeapIterator implements DbFileIterator {
    private int pageNo;
    private Iterator<Tuple>pageIt;
    private boolean opened;
    private TransactionId tid;
    private int numPages;
    private int id;

    public HeapIterator(TransactionId tid, int nPages, int id) {
        this.tid = tid;
        this.numPages = nPages;
        this.id = id;
    }

    public void open()
            throws DbException, TransactionAbortedException, IOException, InterruptedException {
        pageNo = 0;

        int i = findNextIteratorIndex(0);

        if (i == -1)
            pageIt = null;
        else
            pageIt = getIteratorAtIndex(i);

        opened = true;
    }

    public boolean hasNext()
            throws DbException, TransactionAbortedException, IOException, InterruptedException {
        boolean result = false;

        if (!opened)
            ;
        else if (pageIt == null)
            ;
        else if (pageIt.hasNext())
            result = true;
        else if (pageNo >= numPages)
            ;

        int i = findNextIteratorIndex(pageNo + 1);
        if (i == -1)
            ;
        else
            result = true;

        return result;
    }

    private Iterator<Tuple> getIteratorAtIndex(int i)
            throws DbException, TransactionAbortedException, NoSuchElementException, IOException, InterruptedException {
        HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.id, i), null );
        return currentPage.iterator();
    }

    private int findNextIteratorIndex(int i)
            throws DbException, TransactionAbortedException, NoSuchElementException, IOException, InterruptedException {
        for(; i < numPages; i++) {
            if(getIteratorAtIndex(i).hasNext())
                return i;
        }

        return -1;
    }

    public Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException, IOException, InterruptedException {

        if (!opened || pageIt == null || pageNo >= numPages)
            throw new NoSuchElementException();

        if (pageIt.hasNext())
            return pageIt.next();

        pageNo++;
        int i = findNextIteratorIndex(pageNo);
        if (i == -1)
            throw new NoSuchElementException("No Tuples Left");
        else {
            pageNo = i;
            pageIt = getIteratorAtIndex(i);
            return pageIt.next();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException, InterruptedException {
        close();
        open();
    }

    public void close() {
        opened = false;
    }
}