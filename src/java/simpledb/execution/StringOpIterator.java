package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

public class StringOpIterator implements OpIterator{
    private StringAggregator stringAggregator;
    private List<Tuple> m_list = new ArrayList<Tuple>();
    private Iterator<Tuple> iterator;
    StringOpIterator(StringAggregator stringAggregator)
    {
        this.stringAggregator = stringAggregator;
        if(stringAggregator.what == Aggregator.Op.COUNT)
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
        return stringAggregator.tupleDesc;
    }

    @Override
    public void close() {
        iterator = null;

    }
}