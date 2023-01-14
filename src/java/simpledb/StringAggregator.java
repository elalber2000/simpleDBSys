package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private String gbfieldname;
	private String afieldname;

	private HashMap<Field, Integer> values;

	/**
	 * Aggregate constructor
	 * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param afield the 0-based index of the aggregate field in the tuple
	 * @param what aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		values = new HashMap<>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {

		Field gb;

		if (gbfield == Aggregator.NO_GROUPING) {
			gb = new IntField(gbfield);
			afieldname = tup.getTupleDesc().getFieldName(afield);
		} else {
			gb = tup.getField(gbfield);
			gbfieldname = tup.getTupleDesc().getFieldName(gbfield);
		}

		values.put(gb, values.getOrDefault(gb, 0) + 1);
	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal,
	 *   aggregateVal) if using group, or a single (aggregateVal) if no
	 *   grouping. The aggregateVal is determined by the type of
	 *   aggregate specified in the constructor.
	 */
	public OpIterator iterator() {


		Type[] types = new Type[gbfield == Aggregator.NO_GROUPING ? 1 : 2];
		String[] names = new String[gbfield == Aggregator.NO_GROUPING ? 1 : 2];

		if (gbfield == Aggregator.NO_GROUPING) {
			types[0] = Type.INT_TYPE;
			names[0] = afieldname;
		} else {
			types[0] = gbfieldtype;
			types[1] = Type.INT_TYPE;
			names[0] = gbfieldname;
			names[1] = afieldname;
		}

		TupleDesc td = new TupleDesc(types, names);
		List<Tuple> res = new ArrayList<>();

		for (Map.Entry<Field, Integer> temp : values.entrySet()) {
			Tuple t = new Tuple(td);
			if (gbfield == Aggregator.NO_GROUPING) {
				t.setField(0, new IntField(temp.getValue()));
			} else {
				t.setField(0, temp.getKey());
				t.setField(1, new IntField(temp.getValue()));
			}
			res.add(t);
		}

		return new TupleIterator(td, res);

	}

}
