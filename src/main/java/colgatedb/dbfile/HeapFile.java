package colgatedb.dbfile;

import colgatedb.AccessManager;
import colgatedb.Database;
import colgatedb.page.PageId;
import colgatedb.page.SimplePageId;
import colgatedb.page.SlottedPage;
import colgatedb.page.SlottedPageMaker;
import colgatedb.transactions.Permissions;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import javafx.css.CssParser;

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
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with SlottedPage. The format of SlottedPages is described in the javadocs
 * for SlottedPage.
 *
 * @see SlottedPage
 */
public class HeapFile implements DbFile {

    private final SlottedPageMaker pageMaker;
    private TupleDesc td;
    private int tableid;
    private int numPages;
    private AccessManager accessmanager;

    /**
     * Creates a heap file.
     * @param td the schema for records stored in this heapfile
     * @param pageSize the size in bytes of pages stored on disk (needed for PageMaker)
     * @param tableid the unique id for this table (needed to create appropriate page ids)
     * @param numPages size of this heapfile (i.e., number of pages already stored on disk)
     */
    public HeapFile(TupleDesc td, int pageSize, int tableid, int numPages) {
        pageMaker = new SlottedPageMaker(td,pageSize);
        this.numPages = numPages;
        this.tableid = tableid;
        this.td = td;
        accessmanager = Database.getAccessManager();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numPages;
    }

    @Override
    public int getId() {
        return tableid;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }


    /**
     * Finds an appropriate page to insert a tuple or allocates a new page if pages in the heapfile are all full
     * @return the page that can be inserted
     */
    private SlottedPage findAppropriatePage(TransactionId tid){
        // finds a page with empty slots in it
        for (int i = 0; i < numPages; i ++){
            SimplePageId pid = new SimplePageId(tableid,i);
            boolean justacquire = false;
            if(accessmanager.holdsLock(tid,pid, Permissions.READ_ONLY)){
                justacquire = true;
            }
            try{
                accessmanager.acquireLock(tid, pid, Permissions.READ_ONLY);
            }
            catch (TransactionAbortedException e){
            }
            SlottedPage temp = (SlottedPage) accessmanager.pinPage(tid,pid,pageMaker);
            if (temp.getNumEmptySlots() != 0){
                try{
                    accessmanager.acquireLock(tid, pid, Permissions.READ_WRITE);
                }
                catch (TransactionAbortedException e){
                }
                return temp;
            }
            if(!justacquire){
                accessmanager.releaseLock(tid,pid);
            }
            accessmanager.unpinPage(tid, temp,false);
        }
        // No empty slots available, needs to allocate a new page
        SimplePageId newpid= new SimplePageId(tableid,numPages);
        numPages ++;
        accessmanager.allocatePage(newpid);
        try{
            accessmanager.acquireLock(tid, newpid, Permissions.READ_WRITE);
        }
        catch (TransactionAbortedException e){
        }
        SlottedPage newpage = (SlottedPage) accessmanager.pinPage(tid,newpid,pageMaker);
        return newpage;
    }


    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        SlottedPage page = findAppropriatePage(tid);
        page.insertTuple(t);
        accessmanager.unpinPage(tid,page,true);
    }


    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        SlottedPage page = (SlottedPage) accessmanager.pinPage(tid,pid,pageMaker);
        page.deleteTuple(t);
        accessmanager.unpinPage(tid, page,true);
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {

        private boolean isopen;

        private int currentpage;

        private SlottedPage page;

        private Iterator<Tuple> pageiterator;

        private TransactionId tid;

        public HeapFileIterator(TransactionId tid) {
            currentpage = 0;
            this.tid = tid;
        }

        @Override
        public void open() throws TransactionAbortedException {
            isopen = true;
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            if (!isopen){
                return false;
            }
            if (currentpage == numPages){
                return false;
            }
            // sets up a new page to iterate
            if (page == null){
                SimplePageId pid = new SimplePageId(tableid,currentpage);
                page = (SlottedPage) accessmanager.pinPage(tid,pid,pageMaker);
                pageiterator = page.iterator();
            }
            boolean iftuple = pageiterator.hasNext();
            if (iftuple){
                return iftuple;
            }
            // no available slots in this page, needs to iterate the next page
            else {
                accessmanager.unpinPage(tid,page,false);
                page = null;
                currentpage ++;
                return hasNext();
            }
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            if (!hasNext()){
                throw new NoSuchElementException();
            }
            return pageiterator.next();
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            currentpage = 0;
            if (page != null){
                accessmanager.unpinPage(tid, page, false);
                page = null;
            }
            pageiterator = null;
        }

        @Override
        public void close() {
            if (page != null){
                accessmanager.unpinPage(tid, page,false);
                page = null;
                pageiterator = null;
            }
            isopen = false;
        }
    }

}
