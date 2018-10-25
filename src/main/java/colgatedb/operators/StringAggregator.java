package colgatedb.operators;

import colgatedb.page.Page;
import colgatedb.tuple.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {


    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, AggregateFields> lists;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        lists = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupby = tup.getField(gbfield);
        if(lists.get(groupby) != null){
            AggregateFields aggregatefield = lists.get(groupby);
            aggregatefield.count ++;
        }
        else{
            AggregateFields aggregate = new AggregateFields(tup.getField(gbfield).toString());
            aggregate.count = 1;
            lists.put(groupby,aggregate);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> iterable = new ArrayList<>();
        TupleDesc td;
        if(gbfieldtype == null) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        else{
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        for(Field fieldname: lists.keySet()){
            AggregateFields temp = lists.get(fieldname);
            Tuple tuple = new Tuple(td);
            IntField intField = new IntField(temp.count);
            if(gbfieldtype == null) {
                tuple.setField(0,intField);
            }
            else{
                tuple.setField(0,fieldname);
                tuple.setField(1,intField);
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
        public int count;

        public AggregateFields(String groupVal) {
            this.groupVal = groupVal;
            count = 0;
        }
    }
}
