package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private DbIterator aggIterator;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private TupleDesc td;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
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
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        // variables for our td and building our td
        Type[] typeAr;
        String[] fieldAr;
        if(gfield != Aggregator.NO_GROUPING){
            Type groupType = child.getTupleDesc().getFieldType(gfield);
            String groupField = child.getTupleDesc().getFieldName(gfield);
            typeAr = new Type[]{groupType, Type.INT_TYPE};
            fieldAr = new String[]{groupField, child.getTupleDesc().getFieldName(afield)};
        }
        else{
            fieldAr = new String[]{child.getTupleDesc().getFieldName(afield)};
            typeAr = new Type[]{Type.INT_TYPE};
        }
        td = new TupleDesc(typeAr,fieldAr);

        // create our aggregator
        if(child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE){
            aggregator = new IntegerAggregator(gfield,td.getFieldType(gfield),afield,aop);
        }
        else{
            aggregator = new StringAggregator(gfield,td.getFieldType(gfield),afield,aop);
        }

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        if(gfield == Aggregator.NO_GROUPING){
            return Aggregator.NO_GROUPING;
        }
        else{
            return gfield;
        }
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        if(gfield == Aggregator.NO_GROUPING){
            return null;
        }
        else{
            return child.getTupleDesc().getFieldName(gfield);
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return -1;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);

    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        child.open();
        while(child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();
        aggIterator = aggregator.iterator();
        aggIterator.open();
    }

    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(aggIterator.hasNext()){
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(aggIterator.hasNext()){
            return aggIterator.next();
        }
        else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggIterator.rewind();
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
        return td;
    }

    public void close() {
        aggIterator.close();
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    }

}
