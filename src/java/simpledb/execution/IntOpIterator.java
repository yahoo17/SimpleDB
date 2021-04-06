package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

public class IntOpIterator implements OpIterator{
    private IntegerAggregator integerAggregator;
    private Iterator<Tuple> iterator;
    public List<Tuple> m_list = new ArrayList<>();

    IntOpIterator(IntegerAggregator integerAggregator)
    {
        this.integerAggregator = integerAggregator;
        if (integerAggregator.what == Aggregator.Op.SUM) {
            for (Map.Entry<Field, Integer> entry : integerAggregator.mpSum.entrySet()) {
                int sum = entry.getValue();
                Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(sum));
                m_list.add(tuple);
            }
        }
        else if (integerAggregator.what == Aggregator.Op.AVG)
        {
            for (Map.Entry<Field, Integer> entry : integerAggregator.mpCount.entrySet()) {
                int count = entry.getValue();
                int sum = integerAggregator.mpSum.get(entry.getKey());
                Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(sum/count));
                m_list.add(tuple);
            }

        }else if (integerAggregator.what == Aggregator.Op.MAX)
        {
            for (Map.Entry<Field, Integer> entry : integerAggregator.mpMax.entrySet()) {
                int max = entry.getValue();
                Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(max));
                m_list.add(tuple);
            }
        }else if(integerAggregator.what == Aggregator.Op.MIN)
        {
            for (Map.Entry<Field, Integer> entry : integerAggregator.mpMin.entrySet()) {
                int min = entry.getValue();
                Tuple tuple = new Tuple(integerAggregator.tupleDesc);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(min));
                m_list.add(tuple);
            }

        }else if(integerAggregator.what == Aggregator.Op.COUNT){
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
        if(hasNext())
            return iterator.next();
        else
            return null;
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
