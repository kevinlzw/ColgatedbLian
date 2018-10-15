package colgatedb.page;

import colgatedb.tuple.Tuple;
import colgatedb.tuple.RecordId;
import colgatedb.tuple.TupleDesc;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */

/**
 * SlottedPage stores a collection of fixed-length tuples, all having the same schema.
 * Upon insertion, a tuple is assigned to a slot.  The number of slots available depends on
 * the size of the page and the schema of the tuples.
 */
public class SlottedPage implements Page {

    private final PageId pid;
    private final TupleDesc td;
    private final int pageSize;
    private int numberslot;
    private Tuple[] tuples;
    private BitSet header;


    // ------------------------------------------------
    // oldData fields:
    // these are used for logging and recovery -- you can ignore for now
    private final Byte oldDataLock = (byte) 0;
    byte[] oldData;
    // ------------------------------------------------

    /**
     * Constructs empty SlottedPage
     * @param pid  page id to assign to this page
     * @param td   the schema for tuples held on this page
     * @param pageSize the size of this page
     */
    public SlottedPage(PageId pid, TupleDesc td, int pageSize) {
        this.pid = pid;
        this.td = td;
        this.pageSize = pageSize;
        this.numberslot = SlottedPageFormatter.computePageCapacity(pageSize,td);
        tuples = new Tuple[numberslot];
        header = new BitSet(numberslot);
        setBeforeImage();
    }

    /**
     * Constructs SlottedPage with its data initialized according to last parameter
     * @param pid  page id to assign to this page
     * @param td   the schema for tuples held on this page
     * @param pageSize the size of this page
     * @param data data with which to initialize page content
     */
    public SlottedPage(PageId pid, TupleDesc td, int pageSize, byte[] data) {
        this(pid, td, pageSize);
        setPageData(data);
        setBeforeImage();
    }

    @Override
    public PageId getId() {
        return pid;
    }

    /**
     * @param slotno the slot number
     * @return true if this slot is used (i.e., is occupied by a tuple).
     */
    public boolean isSlotUsed(int slotno) {
        return header.get(slotno);
    }

    /**
     *
     * @param slotno the slot number
     * @return true if this slot is empty (i.e., is not occupied by a tuple).
     */
    public boolean isSlotEmpty(int slotno) {
        return !isSlotUsed(slotno);
    }

    /**
     * @return the number of slots this page can hold.  Determined by
     * the page size and the schema (TupleDesc).
     */
    public int getNumSlots() {
         return numberslot;
    }

    /**
     * @return the number of slots on this page that are empty.
     */
    public int getNumEmptySlots() {
        return numberslot - header.cardinality();
    }

    /**
     * @param slotno the slot of interest
     * @return returns the tuple at given slot
     * @throws PageException if slot is empty
     */
    public Tuple getTuple(int slotno) {
        if(!header.get(slotno)){
            throw new PageException("The slot is empty");
        }
        return tuples[slotno];
    }

    /**
     * Adds the specified tuple to specific slot in page.
     * <p>
     * The tuple should be updated to reflect that it is now stored on this page
     * (hint: set its RecordId).
     *
     * @param slotno the slot into which this tuple should be inserted
     * @param t The tuple to add.
     * @throws PageException if the slot is full or TupleDesc of
     *                          passed tuple is a mismatch with TupleDesc of this page.
     */
    public void insertTuple(int slotno, Tuple t) {
        if(header.get(slotno)){
            throw new PageException("The slot is full");
        }
        if(header.cardinality() == numberslot){
            throw new PageException("The page is full");
        }
        if(!t.getTupleDesc().equals(td)){
            throw new PageException("Passed tuple is a mismatch with TupleDesc of this page");
        }
        header.set(slotno);
        tuples[slotno] = t;
        tuples[slotno].setRecordId(new RecordId(pid,slotno));
    }

    /**
     * Adds the specified tuple to the page into an available slot.
     * <p>
     * The tuple should be updated to reflect that it is now stored on this page
     * (hint: set its RecordId).
     *
     * @param t The tuple to add.
     * @throws PageException if the page is full (no empty slots) or TupleDesc of
     *                          passed tuple is a mismatch with TupleDesc of this page.
     */
    public void insertTuple(Tuple t) throws PageException {
        int index = header.nextClearBit(0);
        insertTuple(index,t);
    }

    /**
     * Delete the specified tuple from the page; the tuple should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws PageException if this tuple doesn't have a record id, is not on this page, or tuple
     *                          slot is already empty.
     */
    public void deleteTuple(Tuple t) throws PageException {
        if(t.getRecordId() == null){
            throw new PageException("This tuple doesn't have a record id");
        }
        if(t.getRecordId().getPageId() != pid){
            throw new PageException("This tuple is not on this page");
        }
        if(!header.get(t.getRecordId().tupleno()) ){
            throw new PageException("The tuple slot is already empty");
        }
        header.set(t.getRecordId().tupleno(),false);
        t.setRecordId(null);
    }


    /**
     * Creates an iterator over the (non-empty) slots of the page.
     *
     * @return an iterator over all tuples on this page
     * (Note: calling remove on this iterator throws an UnsupportedOperationException)
     */
    public Iterator<Tuple> iterator() {
        return new pageIterator();
    }


    class pageIterator implements Iterator<Tuple> {

        private int currIdx;

        public pageIterator() {
            currIdx = header.nextSetBit(0);
        }

        @Override
        public boolean hasNext() {
            return currIdx < numberslot && currIdx >=0;
        }

        @Override
        public Tuple next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple next = tuples[currIdx];
            currIdx = header.nextSetBit(currIdx+1);
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("my data can't be modified!");
        }
    }





    @Override
    public byte[] getPageData() {
         return SlottedPageFormatter.pageToBytes(this,td,pageSize);
    }

    /**
     * Fill the contents of this according to the data stored in byte array.
     * @param data
     */
    private void setPageData(byte[] data) {
         SlottedPageFormatter.bytesToPage(data,this,td);
    }

    @Override
    public Page getBeforeImage() {
        byte[] oldDataRef;
        synchronized (oldDataLock) {
            oldDataRef = Arrays.copyOf(oldData, oldData.length);
        }
        return new SlottedPage(pid, td, pageSize, oldDataRef);
    }

    @Override
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

}
