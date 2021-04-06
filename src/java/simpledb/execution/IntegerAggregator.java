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

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    public Op what;

    // sum score, groupby college
    public Map<Field,Integer> mpSum = new HashMap<>();
    public Map<Field,Integer> mpCount = new HashMap<>();
    public Map<Field,Integer> mpMax = new HashMap<>();
    public Map<Field,Integer> mpMin = new HashMap<>();
    //average can be compute;



    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype =gbfieldtype;
        this.what = what;
        Type [] types= {gbfieldtype,Type.INT_TYPE};
        String [] names = {"name1", "name2"};
        tupleDesc = new TupleDesc(types,names);
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

        Field key = tup.getField(gbfield);

        if(mpCount.get(key) == null)
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
            mpMin.put(key,Math.min(mpMax.get(key),value));
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
    private TupleDesc tupleDesc;

    private class IntOpIterator implements OpIterator{
        IntegerAggregator integerAggregator;
        private Iterator<Tuple> iterator;

        IntOpIterator(IntegerAggregator integerAggregator)
        {
            this.integerAggregator = integerAggregator;

            if (integerAggregator.what == Op.SUM) {
                for (Map.Entry<Field, Integer> entry : integerAggregator.mpSum.entrySet()) {
                    int sum = entry.getValue();
                    Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(sum));
                    m_list.add(tuple);
                }
            }
            else if (integerAggregator.what == Op.AVG)
            {
                for (Map.Entry<Field, Integer> entry : integerAggregator.mpCount.entrySet()) {
                    int count = entry.getValue();
                    int sum = integerAggregator.mpSum.get(entry.getKey());
                    Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(sum/count));
                    m_list.add(tuple);
                }

            }else if (integerAggregator.what == Op.MAX)
            {
                for (Map.Entry<Field, Integer> entry : integerAggregator.mpMax.entrySet()) {
                    int max = entry.getValue();
                    Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(max));
                    m_list.add(tuple);
                }
            }else if(integerAggregator.what == Op.MIN)
            {
                for (Map.Entry<Field, Integer> entry : integerAggregator.mpMin.entrySet()) {
                    int min = entry.getValue();
                    Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(min));
                    m_list.add(tuple);
                }

            }else if(integerAggregator.what == Op.COUNT){
                for (Map.Entry<Field, Integer> entry : integerAggregator.mpCount.entrySet()) {
                    int count = entry.getValue();
                    Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(count));
                    m_list.add(tuple);
                }
            }
            else
            {
                Debug.log("this is something wrong! with IntegerAggregator.java un implement in lab7");
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
            return integerAggregator.tupleDesc;
        }

        @Override
        public void close() {
            iterator = null;

        }
    }
    private List<Tuple> m_list = new ArrayList<>();
    public OpIterator iterator() {

        return new IntOpIterator(this);
    }

}
