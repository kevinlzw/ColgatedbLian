package colgatedb.page;

import colgatedb.tuple.Field;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

import java.io.*;
import java.util.Iterator;

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
 * SlottedPageFormatter is responsible for translating a SlottedPage to and from a
 * byte array representation.
 * <p>
 * A SlottedPage has an array of slots, each of which can hold one tuple and all tuples have the same exact size,
 * that is determined by the TupleDesc for this page.
 * <p>
 * The page format has three components:
 * (a) header
 * (b) payload
 * (c) zeroed out excess bytes
 * The header is a bitmap, with one bit per tuple slot. If the bit corresponding to a particular slot is 1, it
 * indicates that the slot is occupied; if it is 0, the slot is considered empty.
 * <p>
 * The layout of the header requires some explanation.  The first byte of the header represents slots 0..7, the second
 * byte is slots 8..15, and so on.  However, within a byte, the least significant bit of represents the lowest slot
 * value. Thus, suppose the first byte looked like this:
 * bits:  10010110
 * this indicates that slots 1, 2, 4, and 7 are occupied and slots 0, 3, 5, and 6 are empty.  In other words, the bits
 * for the slots are arranged according to following pattern:
 *
 * 7,6,5,4,3,2,1,0  15,14,13,12,11,10,9,8  23,22,21,20,19,18,17,16  and so on.
 *
 * <p>
 * The payload is the data itself.  The tuples of the page are written out in slot order from slot 0 to slot N-1 where
 * N is the number of slots on the page.  If the slot is occupied, the bytes for that slot consist of the data for each
 * field in th tuple, written out in order.  Let k be the number of bytes required to store a tuple.  If the slot is
 * empty slot, then k bytes of zeros are written out.
 * <p>
 * After the last slot is written, there may be excess bytes.  These are just zeroed out.
 */
public class SlottedPageFormatter {

    /**
     * The tuple capacity is computed as follows:
     * - Each tuple has a tuple size (determined by the TupleDesc), which is measured in bytes.
     * - There are 8 bits in a byte.
     * - Additionally, each tuple requires 1 bit in header.
     * - Thus, given the pageSize (measured in bytes) we can store at most.
     *     floor((pageSize *8) / (tuple size * 8 + 1))
     *   tuples on a page.
     * @return number of tuples that this page can hold
     */
    public static int computePageCapacity(int pageSize, TupleDesc td) {
        return (pageSize * 8) / (td.getSize() * 8 + 1);
    }

    /**
     * The size of the header is the number of bytes needed to store the header given that
     * each slot requires one bit.  This is equal to ceiling( numSlots / 8 ).
     *
     * @param numSlots
     * @return the size of the header in bytes.
     */
    public static int getHeaderSize(int numSlots) {
        return (int) Math.ceil(numSlots / 8.0);
    }

    /**
     * Write out the page to bytes.  See the javadoc at the top of file for byte format description.
     * @param page the page to write
     * @param td the TupleDesc that describes the tuples on the page
     * @param pageSize the size of the page
     * @return
     */
    public static byte[] pageToBytes(SlottedPage page, TupleDesc td, int pageSize) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(pageSize);
            DataOutputStream dos = new DataOutputStream(baos);
            // writes header into bytes
            byte[] header = new byte[getHeaderSize(page.getNumSlots())];
            for(int i = 0; i < page.getNumSlots();i++){
                markSlot(i,header,page.isSlotUsed(i));
            }
            dos.write(header);

            // writes payload into bytes
            for(int i = 0; i < page.getNumSlots(); i ++){
                // the slot is used and writes the data to bytes
                if(page.isSlotUsed(i)) {
                    Iterator<Field> fieldIterator = page.getTuple(i).fields();
                    while (fieldIterator.hasNext()) {
                        fieldIterator.next().serialize(dos);
                    }
                }
                // the slot is unused and writes 0
                else{
                    byte[] temp = new byte[td.getSize()];
                    dos.write(temp);
                }
            }

            // writes excess bytes
            int extra = pageSize - getHeaderSize(page.getNumSlots()) - td.getSize() * page.getNumSlots();
            byte[] excess = new byte[extra];
            dos.write(excess);

            baos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new PageException(e);
        }
    }

    /**
     * Populate the empty page with data that is read from the given bytes.  See the javadoc at the top of file
     * for byte format description.
     * @param bytes bytes representing page data
     * @param emptyPage an initially emptyPage to be populated
     * @param td the TupleDesc of tuples on this page
     */
    public static void bytesToPage(byte[] bytes, SlottedPage emptyPage, TupleDesc td) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
            int headersize = getHeaderSize(emptyPage.getNumSlots());
            // writes bytes into page's header
            byte [] header = new byte[headersize];
            for(int i = 0; i < headersize; i ++){
                header[i] = dis.readByte();
            }
            // writes bytes into page's payload
            for(int i = 0; i < emptyPage.getNumSlots(); i ++){
                Tuple tuple = new Tuple(td);
                if(isSlotUsed(i,header)) {
                    for (int j = 0; j < td.numFields(); j++) {
                        Type temp = td.getFieldType(j);
                        Field field = temp.parse(dis);
                        tuple.setField(j, field);
                    }
                    emptyPage.insertTuple(i, tuple);
                }
                // skips bytes if the slot is occupied
                else{
                    dis.skipBytes(td.getSize());
                }
            }
            dis.close();
        } catch (IOException e) {
            throw new PageException(e);
        }
    }


    /**
     * Checks whether a slot in the header is used or not.  Optional helper method.
     * @param i slot index to check
     * @param header a byte header, formatted as described in the javadoc at the top.
     * @return
     */
    private static boolean isSlotUsed(int i, byte[] header) {
        int block = i / 8;
        int num = i % 8;
        // transforms the bytes into String
        String binary = String.format("%8s", Integer.toBinaryString(header[block] & 0xFF)).replace(' ', '0');
        // checks if the specific index is occupied
        char usedslot = binary.charAt(7 - num);
        int slot = Character.getNumericValue(usedslot);
        return slot == 1;
    }

    /**
     * Marks a slot in the header as used or not.  Optional helper method.
     * @param i slot index
     * @param header a byte header, formatted as described in the javadoc at the top.
     * @param isUsed if true, slot should be set to 1; if false, set to 0
     */
    private static void markSlot(int i, byte[] header, boolean isUsed) {
        int block = i / 8;
        int num = i % 8;
        // transforms the bytes into String
        String binary = String.format("%8s", Integer.toBinaryString(header[block] & 0xFF)).replace(' ', '0');
        StringBuilder binarybuilder = new StringBuilder(binary);
        // marks the slot as occupied or not
        binarybuilder.setCharAt(7 - num,(isUsed ? '1':'0'));
        int result = Integer.parseInt(binarybuilder.toString(),2);
        header[block] = (byte) result;
    }
}
