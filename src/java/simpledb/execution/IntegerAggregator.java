package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    public Op what;

    public TupleDesc tupleDesc;


    // sum score, groupby college
    public Map<Field,Integer> mpSum = new HashMap<>();
    public Map<Field,Integer> mpCount = new HashMap<>();
    public Map<Field,Integer> mpMax = new HashMap<>();
    public Map<Field,Integer> mpMin = new HashMap<>();
    //average can be compute;

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
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype =gbfieldtype;
        this.what = what;
        if(gbfield==NO_GROUPING || gbfieldtype == null)
        {
            // Debug.log("No_grouping");
            Type [] types= {Type.INT_TYPE};
            String [] names = {"name0"};
            tupleDesc = new TupleDesc(types,names);
        }
        else
        {
            Type [] types= {gbfieldtype,Type.INT_TYPE};
            String [] names = {"name0", "name1"};
            tupleDesc = new TupleDesc(types,names);
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField field = (IntField) tup.getField(afield);
        Integer value = field.getValue();

        Field key = gbfield==-1 ? null: tup.getField(gbfield);

        if(key != null && key.getType()!=this.gbfieldtype){
            throw new IllegalArgumentException("Given tuple has wrong type");
        }

        if(!mpCount.containsKey(key))
        {
            mpCount.put(key,1);
            mpSum.put(key,value);
            mpMax.put(key,value);
            mpMin.put(key,value);
        }
        else
        {
            mpCount.put(key,mpCount.get(key)+1);
            mpMax.put(key,Math.max(mpMax.get(key),value));
            mpMin.put(key,Math.min(mpMin.get(key),value));
            mpSum.put(key,mpSum.get(key)+value);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */


    public OpIterator iterator() {
        return new IntOpIterator(this);
    }

}
