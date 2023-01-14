package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

	private static final long serialVersionUID = 1L;
	private OpIterator child;
	private Aggregator.Op aop;
	private int gfield;
	private int afield;
	private Aggregator agg;
	private OpIterator iterator;

	/**
	 * Constructor.
	 * 
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
	 * you with your implementation of readNext().
	 * 
	 * 
	 * @param child
	 *            The OpIterator that is feeding us tuples.
	 * @param afield
	 *            The column over which we are computing an aggregate.
	 * @param gfield
	 *            The column over which we are grouping the result, or -1 if
	 *            there is no grouping
	 * @param aop
	 *            The aggregation operator to use
	 */
	public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
		this.gfield = gfield;
		this.child = child;
		this.afield = afield;
		this.aop = aop;

		Type grType = child.getTupleDesc().getFieldType(gfield);
		Type aggType = child.getTupleDesc().getFieldType(afield);

		if(aggType == Type.INT_TYPE)     		
			agg = new IntegerAggregator(gfield, grType, afield, aop);
		else if (aggType == Type.STRING_TYPE) 
			agg = new StringAggregator(gfield, grType, afield, aop);
	}

	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 *         field index in the <b>INPUT</b> tuples. If not, return
	 *         {@link simpledb.Aggregator#NO_GROUPING}
	 * */
	public int groupField() {
		return gfield;
	}

	/**
	 * @return If this aggregate is accompanied by a group by, return the name
	 *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
	 *         null;
	 * */
	public String groupFieldName() {

		String res = null;

		if (gfield != Aggregator.NO_GROUPING)
			res = child.getTupleDesc().getFieldName(gfield);

		return res;
	}

	/**
	 * @return the aggregate field
	 * */
	public int aggregateField() {
		return afield;
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

		while(child.hasNext()) {
			agg.mergeTupleIntoGroup(child.next());
		}

		iterator = agg.iterator();
		iterator.open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate. If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {

		if(iterator.hasNext()) 
			return iterator.next();

		return null;

	}

	public void rewind() throws DbException, TransactionAbortedException {
		iterator.rewind();
		child.rewind();
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
		
		TupleDesc td = child.getTupleDesc();
		String aname = td.getFieldName(afield);

		Type[] types = new Type[afield == Aggregator.NO_GROUPING ? 1 : 2];
		String[] names = new String[afield == Aggregator.NO_GROUPING ? 1 : 2];
		
		String fieldName = null;
		if(aname!=null)
			fieldName = nameOfAggregatorOp(aop) + "(" + aname + ")";

		if(afield == Aggregator.NO_GROUPING){
			types[0] = td.getFieldType(afield);
			names[0] = fieldName;
		} else {

			types[0] = td.getFieldType(gfield);
			types[1] = td.getFieldType(afield);
			names[0] = td.getFieldName(gfield);
			names[1] = fieldName;
		}
		
		return new TupleDesc(types, names);
	}

	public void close() {
		super.close();
		iterator.close();
		child.close();
	}

	@Override
	public OpIterator[] getChildren() {
		return new OpIterator[] { child };
	}

	@Override
	public void setChildren(OpIterator[] children) {
		this.child = children[0];
	}

}
