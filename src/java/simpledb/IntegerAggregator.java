package simpledb;

import java.lang.*;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */

public class IntegerAggregator implements Aggregator {
	
    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private ArrayList<Map.Entry<Field, Field>> groups;
    private HashMap<Field, Field> aggregates;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.groups = new ArrayList<Map.Entry<Field, Field>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	IntField key;
        if (this.gbfield == Aggregator.NO_GROUPING) {
        	key = new IntField(Aggregator.NO_GROUPING);
        } else {
        	key = (IntField) tup.getField(this.gbfield);
        }
        //System.out.println("key: " + key);
        //System.out.println("value: " + tup.getField(this.afield));
        groups.add(new AbstractMap.SimpleEntry<Field, Field>(key, tup.getField(this.afield)));
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	Tuple tuple = null;
    	TupleDesc td = null;
    	ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        this.aggregates = new HashMap<Field, Field>();
        
        // populate aggregates hashmap
        calculateAggregate(this.groups, this.aggregates, this.op);
        Set<Map.Entry<Field, Field>> set = this.aggregates.entrySet();
        
        IntField noGB = new IntField(Aggregator.NO_GROUPING);
        if (this.aggregates.containsKey(noGB)) {
        	td = new TupleDesc(new Type[]{Type.INT_TYPE});
        	tuple = new Tuple(td);
        	tuple.setField(0, noGB);
        } else {
        	for (Map.Entry<Field, Field> item : set) {
        		IntField key = (IntField) item.getKey();
        		IntField value = (IntField) item.getValue();
        		
        		td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
        		tuple = new Tuple(td);
        		tuple.setField(0, key);
        		tuple.setField(1, value);
        		tuples.add(tuple);
        	}
        }
        return new TupleIterator(td, tuples);
    }
    
    private void calculateAggregate(ArrayList<Map.Entry<Field, Field>> groups, HashMap<Field, Field> aggregates, Aggregator.Op op) {
    	HashMap<IntField, Integer> counter = new HashMap<IntField, Integer>(); 
    	
    	for (Map.Entry<Field, Field> group : groups) {
    		IntField key = (IntField) group.getKey();
            //System.out.println(group.getValue());
            IntField value = (IntField) group.getValue();
    		IntField agg = (IntField) aggregates.get(key);
    		
    		// if first item
    		if (agg == null) {
    			if (op == Aggregator.Op.COUNT) {
    				value = new IntField(1);
    			} else if (op == Aggregator.Op.AVG) {
    				counter.put(key, 1);
    			}
    			aggregates.put(key, value);
    			
    		// if there are existing aggregates
    		} else {
    			// sum case
    			if (op == Aggregator.Op.SUM) {
    				aggregates.put(key, new IntField(value.getValue() + agg.getValue()));
    			// max case
    			} else if (op == Aggregator.Op.MAX){
    				if (agg.compare(Predicate.Op.LESS_THAN, value)) {
    					aggregates.put(key, value);
    				}
    			// min case
    			} else if (op == Aggregator.Op.MIN) {
    				if (agg.compare(Predicate.Op.GREATER_THAN, value)) {
    					aggregates.put(key, value);
    				}
    			// count case
    			} else if (op == Aggregator.Op.COUNT) {
    				aggregates.put(key, new IntField(agg.getValue() + 1));
    			// avg case
    			} else if (op == Aggregator.Op.AVG) {
    				Integer count = counter.get(key);
    				int avg = (value.getValue() + count * agg.getValue()) / (count + 1); 
    				aggregates.put(key, new IntField(avg));
    				counter.put(key, count + 1);
    			}
    		}
    	}
    }

}
