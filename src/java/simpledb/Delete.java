package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId ti;
    OpIterator child;
    TupleDesc countTD;
    int status;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.ti = t;
        this.child = child;
        this.countTD = new TupleDesc(new Type[] {Type.INT_TYPE});
        status = -1;
    }

    public TupleDesc getTupleDesc() {
        return countTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        status = 0;
    }

    public void close() {        
    	super.close();
    	child.close();
    	status = -1;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        if(status == -1) {
        	throw new DbException("Not yet opened!");
        }
        child.rewind();
        status = 0;
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
        if(status == 1) return null;
    	if(status == -1) { throw new DbException("Not yet opened!"); }
        
        int noOfInsertions = 0;
        
        try {
        	while(child.hasNext()) {
        		Tuple t = child.next();
        		System.out.println(t);
        		Database.getBufferPool().deleteTuple(ti, t);;
        		noOfInsertions++;
        	}
        }
        catch (IOException e) {
        	throw new DbException(e.toString());
        }
        
        status = 1;
        
        Tuple toRet = new Tuple(countTD);
        toRet.setField(0, new IntField(noOfInsertions));
        return toRet;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
