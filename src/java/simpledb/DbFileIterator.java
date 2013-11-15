package simpledb;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public interface DbFileIterator extends Serializable {
    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open()
        throws DbException, TransactionAbortedException, IOException, InterruptedException;

    /** @return true if there are more tuples available. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException, IOException, InterruptedException;

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException, IOException, InterruptedException;

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException, IOException, InterruptedException;

    /**
     * Closes the iterator.
     */
    public void close();
}
