package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    
    JoinPredicate pred;
    OpIterator child1;
    OpIterator child2;
    Tuple next1;
    Tuple next2;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.pred = p;
        this.child1 = child1;
        this.child2 = child2;
        this.next1 = null;
        this.next2 = null;
    }

    public JoinPredicate getJoinPredicate() {
        return pred;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
    	return child1.getTupleDesc().getFieldName(0);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(0);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
		return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
        child1.open();
        child2.open();
    }

    public void close() {
    	child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	
    	Tuple res = null;
    	
        while(child1.hasNext() || next1 != null) {
            if(next1 == null)
                next1 = child1.next();

            while(child2.hasNext()) {
                next2 = child2.next();
                
                if (pred.filter(next1, next2)) {
                    res = merge(next1, next2);
                    break;
                }
            }

            if (res != null) break;
            
            next1 = null;
            child2.rewind();
        }
        return res;
    }
    
    private Tuple merge(Tuple tp1, Tuple tp2) {
    	
    	Tuple res = new Tuple(getTupleDesc());
    	int size1 = next1.getTupleDesc().numFields();
        int size2 = next2.getTupleDesc().numFields();
        
        for(int i = 0; i < size1; i++)  
            res.setField(i, next1.getField(i));
        for(int i = 0; i < size2; i++)      				
            res.setField(size1 + i, next2.getField(i));
        
    	return res;
    	
    }

    @Override
    public OpIterator[] getChildren() {
    	return new OpIterator[] { this.child1, this.child2 };
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	this.child1 = children[0];
    	this.child2 = children[1];
    }

}
