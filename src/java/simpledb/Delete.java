package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private boolean done = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException, IOException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, IOException {
        Tuple result;
        Type[] t = new Type[1];
        t[0] = Type.INT_TYPE;
        result = new Tuple(new TupleDesc(t));
        result.setField(0, new IntField(0));
        System.out.println("done?: " + done);

        if(!done){
            System.out.println("inside");
            done = true;
            BufferPool theBuffer = Database.getBufferPool();

            int count = 0;
            while(child.hasNext()){
                Tuple tupe = child.next();
                theBuffer.deleteTuple(this.tid, tupe);
                count++;
                //theBuffer.getPage(this.tid,tupe.getRecordId().getPageId(),Permissions.READ_WRITE);
            }

            result.setField(0, new IntField(count));
        }

        System.out.println("field 1 value: " + result.getField(0));
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
