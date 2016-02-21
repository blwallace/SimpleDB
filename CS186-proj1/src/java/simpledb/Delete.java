package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId t;
    DbIterator child;
    TupleDesc td;
    int ticker;
    // boolean to determine if we did funciton multiple tiems
    private boolean first_time;


    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"Deleted Records"});
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
    // custom function to determine if there is another tuple
    public boolean hasNext(){
        if(ticker != 0 || !first_time){
            return false;
        }

        return true;
    }
    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        //create a tuple
        Tuple tup = new Tuple(td);

        //if opened mroe than once, return null
        if(ticker != 0 || !first_time){
            return null;
        }

        // determine if we can insert a tuple from child
        while(child.hasNext()){
            Database.getBufferPool().deleteTuple(t, child.next());
            ticker++;
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
