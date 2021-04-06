package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    public Aggregator aggregator;
    private TupleDesc  tupleDesc;
    private OpIterator it;


    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type gfieldtype = gfield==-1 ? null: child.getTupleDesc().getFieldType(gfield);

        if(child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE)
        {
            aggregator = new IntegerAggregator(gfield,gfieldtype,afield,aop);

        }else if(child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE)
        {
            aggregator = new StringAggregator(gfield,gfieldtype, afield,aop);
        }else
        {
            Debug.log("error with choose correct aggregator");
        }

        List<Type> types = new ArrayList<>();
        List<String > names = new ArrayList<>();

        if (gfieldtype != null) {
            types.add(gfieldtype);
            names.add(this.child.getTupleDesc().getFieldName(this.gfield));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.afield));
        names.add(this.child.getTupleDesc().getFieldName(this.afield));
        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            Debug.log("sum count");
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.tupleDesc = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));


    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */

    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return tupleDesc.getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        if(this.gfield == -1)
            return this.tupleDesc.getFieldName(0);
        else
            return this.tupleDesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        this.child.open();
        while (child.hasNext())
        {
            Tuple t = this.child.next();
            // Debug.log(t.toString());
            aggregator.mergeTupleIntoGroup(t);

        }
        it = aggregator.iterator();
        this.it.open();
        super.open();
        // some code goes here
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
       while (this.it.hasNext())
       {
           return this.it.next();
       }
       return null;

    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        it.rewind();

        // some code goes here
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void close() {
        super.close();
        child.close();
        it.close();

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};

    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        //childs = children;
        this.child = children[0];
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);
        // group field
        if (gfieldtype != null) {
            types.add(gfieldtype);
            names.add(this.child.getTupleDesc().getFieldName(this.gfield));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.afield));
        names.add(this.child.getTupleDesc().getFieldName(this.afield));
        if (aop.equals(Aggregator.Op.SUM_COUNT)) {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.tupleDesc = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));

    }

}
