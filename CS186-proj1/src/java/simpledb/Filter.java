package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p;
    private DbIterator child;
    private DbIterator[] children;
    private boolean open = false;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */

    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    //@Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child.open();
    }

    //@Override
    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {

        child.rewind();
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
        Tuple next;
        do {
            if (child.hasNext())
                next = child.next();
            else
                return null;

        } while (!p.filter(next));
        return next;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        //DbIterator[] result = new DbIterator[1];
        //result[0]= child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.children = children;
    }

}
