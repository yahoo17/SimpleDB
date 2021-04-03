package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate m_predicate;
    private OpIterator m_child;

    private OpIterator [] opIterators;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        m_predicate = p;
        m_child = child;
        // some code goes here
    }

    public Predicate getPredicate() {
        // some code goes here
        return m_predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return m_child.getTupleDesc();

    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        m_child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        m_child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (m_child.hasNext())
        {
            Tuple t = m_child.next();
            if( m_predicate.filter(t))
                return t;
        }
        return null;

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator [] opIterators = {m_child};
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        opIterators = children;
    }

}
