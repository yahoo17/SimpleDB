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
    private Op what;

    public Map<Field,Integer> mpCount = new HashMap<>();

    private TupleDesc tupleDesc;


    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        Type [] types = {gbfieldtype, Type.INT_TYPE};
        String [] name ={"name1","name2"};
        tupleDesc = new TupleDesc(types,name);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = tup.getField(gbfield);
        if(mpCount.get(field) == null)
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
    private class StringOpIterator implements OpIterator{
        private StringAggregator stringAggregator;
        private List<Tuple> m_list = new ArrayList<Tuple>();
        private Iterator<Tuple> iterator;
        StringOpIterator(StringAggregator stringAggregator)
        {
            this.stringAggregator = stringAggregator;
            if(stringAggregator.what == Op.COUNT)
            {
                for(Map.Entry<Field,Integer> entry : stringAggregator.mpCount.entrySet())
                {
                    Tuple tuple = new Tuple(stringAggregator.tupleDesc);
                    tuple.setField(0,entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                    m_list.add(tuple);
                }
            }else
            {
                Debug.log("No correct Op stringAggregator");
            }

        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterator = m_list.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            iterator = null;

        }
    }
    public OpIterator iterator() {
        return new StringOpIterator(this);
    }

}
