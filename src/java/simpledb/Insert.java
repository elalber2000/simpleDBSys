package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId ti;
    OpIterator child;
    int tableId;
    TupleDesc countTD;
    int status;
    
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.ti = t;
        this.child = child;
        this.tableId = tableId;
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
     * Inserts tuples read from child into the tableId specified by the
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
        if(status == 1) return null;
    	if(status == -1) { throw new DbException("Not yet opened!"); }
        
        int noOfInsertions = 0;
        
        try {
        	while(child.hasNext()) {
        		Database.getBufferPool().insertTuple(ti, this.tableId, child.next());
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
