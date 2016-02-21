package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    TransactionId t;
    DbIterator child;
    int tableid;
    // used to create our return tuple
    TupleDesc td;
    // used to return count total
    int ticker;
    // boolean to determine if we did funciton multiple tiems
    private boolean first_time;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableid = tableid;
        // create a new tuple description.
        //It returns a one field tuple containing the number of
        // inserted records.
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"Inserted Records"});
        ticker = 0;
        first_time = true;


    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    public boolean hasNext(){
        if(ticker != 0 || !first_time){
            return false;
        }

        return true;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {

        //if opened mroe than once, return null
        if(ticker != 0 || !first_time){
            return null;
        }
        //create a tuple
        Tuple tup = new Tuple(td);

        // determine if we can insert a tuple from child
        while(child.hasNext()){
            try {
                Database.getBufferPool().insertTuple(t,tableid,child.next());
                ticker++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tup.setField(0,new IntField(ticker));
        first_time = false;
        return tup;
    }

    public Tuple next(){
        try {
            return fetchNext();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
