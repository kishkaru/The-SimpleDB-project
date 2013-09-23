package simpledb;
import java.util.*;

public class HeapIterator implements DbFileIterator{

    private ArrayList<Page> pagesList;
    private Iterator<Tuple> tupleIterator = null;
    private int currPage = 0;

    public HeapIterator(ArrayList<Page> theList){
        this.pagesList = theList;
    }

    public void open() throws DbException, TransactionAbortedException{
        Page firstPage = pagesList.get(0);
        tupleIterator = ((HeapPage) firstPage).iterator();
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        boolean result = false;

        if(tupleIterator == null)
            result =  false;
        else if(tupleIterator.hasNext())
            result =  true;
        else if(currPage > pagesList.size() - 1) {
            result = true;
        }

        return result;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws java.util.NoSuchElementException if there are no more tuples
     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(tupleIterator == null)
            throw new NoSuchElementException();
        else if(tupleIterator.hasNext())
            return tupleIterator.next();
        else {
            currPage++;
            Page nextPage = pagesList.get(currPage);
            tupleIterator = ((HeapPage) nextPage).iterator();

            return tupleIterator.next();
        }
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        tupleIterator = null;
    }


}
