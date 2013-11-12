package simpledb;

import java.util.*;
import java.io.IOException;
/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private DbIterator child;
    private ArrayList<Tuple> children = new ArrayList<Tuple>();
    private Iterator<Tuple> iterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException, IOException, InterruptedException {
    	DbIterator child = this.child;
        child.open();
        while (child.hasNext()) {
        	children.add((Tuple) child.next());
        }
        //Collections.sort(children);
        this.iterator = children.iterator();
        super.open();
    }

    public void close() {
        super.close();
        this.iterator = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.iterator = children.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Iterator<Tuple> iterator = this.iterator;
        Predicate predicate = this.predicate;
        Tuple result = null;
        while (iterator != null && iterator.hasNext()) {
        	Tuple next = iterator.next();
        	if (predicate.filter(next)) {
        		result = next;
        		break;
        	}
        }
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
