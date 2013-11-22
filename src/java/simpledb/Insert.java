package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private boolean called = false;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        TupleDesc tupleDescChild = Database.getCatalog().getDbFile(tableid).getTupleDesc();
        if(!child.getTupleDesc().equals(tupleDescChild))
            throw new DbException("TupleDesc doesn't match");

        //System.out.println("child: " + child.getTupleDesc().toString());
        //System.out.println("from mem: " + tupleDescChild.toString());

        this.tid = t;
        this.child = child;
        this.tableId = tableid;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
        //return Database.getCatalog().getDbFile(this.tableId).getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        BufferPool theBuffer = Database.getBufferPool();

        if(this.called)
            return null;
        else
            this.called = true;

        int count = 0;
        while(child.hasNext()){
            Tuple tupe = child.next();
            theBuffer.insertTuple(this.tid,this.tableId,tupe);
            count++;
        }

        Type[] t = new Type[1];
        t[0] = Type.INT_TYPE;
        Tuple result = new Tuple(new TupleDesc(t));
        result.setField(0, new IntField(count));

        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (this.child!=children[0]) {
            this.child = children[0];
        }
    }
}
