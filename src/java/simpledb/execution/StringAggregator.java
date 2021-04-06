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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private Integer gbfield;
    private Type gbfieldtype;
    private Integer afield;
    public Op what;

    public Map<Field,Integer> mpCount = new HashMap<>();

    public TupleDesc tupleDesc;


    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        if(gbfield==NO_GROUPING)
        {
            Debug.log("No_grouping");
            Type [] types= {Type.INT_TYPE};
            String [] names = {"name2"};
            tupleDesc = new TupleDesc(types,names);
        }
        else
        {
            Type [] types= {gbfieldtype,Type.INT_TYPE};
            String [] names = {"name1", "name2"};
            tupleDesc = new TupleDesc(types,names);
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = tup.getField(gbfield);
        if(!mpCount.containsKey(field))
        {
            mpCount.put(field,1);
        }
        else
        {
            mpCount.put(field,mpCount.get(field)+1);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StringOpIterator(this);
    }

}
