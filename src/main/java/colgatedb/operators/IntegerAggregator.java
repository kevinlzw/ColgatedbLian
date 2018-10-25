package colgatedb.operators;

import colgatedb.tuple.*;

import java.util.*;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {


    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, AggregateFields> lists;


    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        lists = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupby = tup.getField(gbfield);
        IntField aIntfield = (IntField) tup.getField(afield);
        int value = aIntfield.getValue();
        if(lists.get(groupby) != null){
            AggregateFields aggregatefield = lists.get(groupby);
            aggregatefield.count ++;
            if(aggregatefield.max < value){
                aggregatefield.max = value;
            }
            if(aggregatefield.min > value){
                aggregatefield.min = value;
            }
            aggregatefield.sum += value;
            aggregatefield.sumCount = aggregatefield.sum / aggregatefield.count;
        }
        else{
            AggregateFields aggregate = new AggregateFields(tup.getField(gbfield).toString());
            aggregate.count = 1;
            aggregate.min = value;
            aggregate.max = value;
            aggregate.sum = value;
            aggregate.sumCount = aggregate.sum;
            lists.put(groupby,aggregate);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> iterable = new ArrayList<>();
        TupleDesc td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
        for(Field fieldname: lists.keySet()){
            AggregateFields temp = lists.get(fieldname);
            Tuple tuple = new Tuple(td);
            if(what.equals(Op.COUNT)){
                tuple.setField(0, fieldname);
                tuple.setField(1, new IntField(temp.count));
            }
            else if(what.equals(Op.MAX)){
                tuple.setField(0, fieldname);
                tuple.setField(1, new IntField(temp.max));
            }
            else if(what.equals(Op.MIN)){
                tuple.setField(0, fieldname);
                tuple.setField(1, new IntField(temp.min));
            }
            else if(what.equals(Op.SUM)){
                tuple.setField(0, fieldname);
                tuple.setField(1, new IntField(temp.sum));
            }
            else if(what.equals(Op.AVG)){
                tuple.setField(0, fieldname);
                tuple.setField(1, new IntField(temp.sumCount));
            }
            iterable.add(tuple);
        }
        return new TupleIterator(td, iterable);
    }

    /**
     * A helper struct to store accumulated aggregate values.
     */
    private class AggregateFields {
        public String groupVal;
        public int min, max, sum, count, sumCount;

        public AggregateFields(String groupVal) {
            this.groupVal = groupVal;
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
            sum = count = sumCount = 0;
        }
    }

}
