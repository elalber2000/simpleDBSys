package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private String gbfieldname;
	private String afieldname;

	private HashMap<Field, Integer> values;
	private HashMap<Field, Integer> avgCount;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
	 *                    null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        the aggregation operator
	 */

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		values = new HashMap<>();
		avgCount = new HashMap<>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		Field gb;
		IntField agg;

		if (gbfield == Aggregator.NO_GROUPING) {
			gb = new IntField(Aggregator.NO_GROUPING);
		} else {
			gb = tup.getField(gbfield);
			gbfieldname = tup.getTupleDesc().getFieldName(gbfield);
		}

		agg = (IntField) tup.getField(afield);
		afieldname = tup.getTupleDesc().getFieldName(afield);

		if (what == Op.COUNT) {
			values.put(gb, values.getOrDefault(gb, 0) + 1);
		}

		if (what == Op.SUM || what == Op.AVG) {
			values.put(gb, values.getOrDefault(gb, 0) + agg.getValue());
		}

		if (what == Op.MAX) {
			values.put(gb, Math.max(values.getOrDefault(gb, agg.getValue()), agg.getValue()));
		}

		if (what == Op.MIN) {
			values.put(gb, Math.min(values.getOrDefault(gb, agg.getValue()), agg.getValue()));
		}

		if (what == Op.AVG) {
			avgCount.put(gb, avgCount.getOrDefault(gb, 0) + 1);
		}

	}

	/**
	 * Create a OpIterator over group aggregate results.
	 * 
	 * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
	 *         using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in the
	 *         constructor.
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

		if (what == Op.AVG) {
			Iterator<Entry<Field, Integer>> itval = values.entrySet().iterator();
			Iterator<Entry<Field, Integer>> itcount = avgCount.entrySet().iterator();
			
			while (itval.hasNext() && itcount.hasNext()) {
				Map.Entry<Field, Integer> val = itval.next();
				Map.Entry<Field, Integer> count = itcount.next();
				
				Tuple t = new Tuple(td);
				
				if (gbfield == Aggregator.NO_GROUPING) {
					t.setField(0, new IntField(val.getValue() / count.getValue()));
				} else {
					t.setField(0, val.getKey());
					t.setField(1, new IntField(val.getValue() / count.getValue()));
				}
				res.add(t);
			}
			
		} else {
			for (Map.Entry<Field, Integer> temp : values.entrySet()) {
				Tuple t = new Tuple(td);
				if (gbfield == Aggregator.NO_GROUPING || what == Op.COUNT) {
					t.setField(0, new IntField(temp.getValue()));
				} else {
					t.setField(0, temp.getKey());
					t.setField(1, new IntField(temp.getValue()));
				}
				res.add(t);
			}
		}

		return new TupleIterator(td, res);
	}

}
