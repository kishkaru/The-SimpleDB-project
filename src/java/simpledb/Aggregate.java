package simpledb;

import java.util.*;
import java.lang.*;
import java.io.IOException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator input;
    private DbIterator aggregator;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.input = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    if(this.gfield == -1)
            return Aggregator.NO_GROUPING;
        else
            return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if (this.gfield == Aggregator.NO_GROUPING)
    		return null;
    	else
            return this.input.getTupleDesc().getFieldName(this.gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    return this.input.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }
    
    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
	    Type aggType = this.input.getTupleDesc().getFieldType(this.afield);
        Type groupByType = null;
        if (this.groupField() != Aggregator.NO_GROUPING)
            groupByType = this.input.getTupleDesc().getFieldType(this.gfield);

        this.input.open();
        super.open();

        Aggregator agg;
	    if (aggType == Type.INT_TYPE) {
	    	agg = new IntegerAggregator(this.gfield, groupByType, this.afield, this.aop);
	    	while (this.input.hasNext())
	    		agg.mergeTupleIntoGroup(this.input.next());
	    }
        else {
	    	agg = new StringAggregator(this.gfield, groupByType, this.afield, this.aop);
	    	while (this.input.hasNext())
	    		agg.mergeTupleIntoGroup(this.input.next());
	    }
        this.input.close();

        this.aggregator = agg.iterator();
        this.aggregator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    Tuple tuple = null;
	    if (this.aggregator.hasNext())
	    	tuple = this.aggregator.next();

	    return tuple;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    this.aggregator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    TupleDesc result = null;
	    TupleDesc td = this.input.getTupleDesc();
	    Type atype = td.getFieldType(this.afield);
	    String aname = td.getFieldName(this.afield);

	    if (this.groupField() == Aggregator.NO_GROUPING)
	    	result = new TupleDesc(new Type[]{atype}, new String[]{aname});

        else {
	    	Type gtype = td.getFieldType(this.gfield);
	    	String gname = td.getFieldName(this.gfield);
	    	result = new TupleDesc(new Type[]{atype, gtype}, new String[]{aname, gname});
	    }

        return result;
    }

    public void close() {
        super.close();
	    this.aggregator.close();
    }

    @Override
    public DbIterator[] getChildren() {
	    return new DbIterator[] {this.aggregator};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    this.input = children[0];
    }
    
}
