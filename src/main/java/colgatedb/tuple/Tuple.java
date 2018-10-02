package colgatedb.tuple;

import java.io.Serializable;
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
 * tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc schema;

    private Map<Integer, Field> tuples;

    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        schema = td;
        tuples = new HashMap<Integer, Field>();
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return schema;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     * @throws RuntimeException if f does not match type of field i.
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public void setField(int i, Field f) {
        if(!schema.getFieldType(i).equals(f.getType())){
            throw new RuntimeException("Field does not match type of field in the schema");
        }
        if(i >= schema.numFields()){
            throw new NoSuchElementException("Input is not a valid field reference");
        }
        tuples.put(i,f);
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Field getField(int i) {
        if(i >= schema.numFields()){
            throw new NoSuchElementException("Input is not a valid field reference");
        }
        return tuples.get(i);
    }

    /**
     * Returns the contents of this tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * <p>
     * where \t is a tab and \n is a newline
     */
    public String toString() {
        String result = "";
        for(int i = 0;i < schema.numFields() - 1;i++){
            result += tuples.get(i).toString() + "\t";
        }
        result += tuples.get(schema.numFields() - 1).toString();
        return result;
    }


    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // hint: use java.util.Arrays.asList to convert array into a list, then return list iterator.
        List<Field> list = new ArrayList<Field>(tuples.values());
        return list.iterator();
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }
}
