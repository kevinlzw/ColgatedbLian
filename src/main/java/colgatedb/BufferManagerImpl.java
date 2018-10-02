package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.awt.*;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

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
public class BufferManagerImpl implements BufferManager {

    private boolean allowEvictDirty = false;  // a flag indicating whether a dirty page is candidate for eviction
    private DiskManager diskManager;
    private HashMap<PageId,Frame> frames;
    private int numPages;
    private PriorityBlockingQueue<Frame> lru;


    /**
     * Construct a new buffer manager.
     * @param numPages maximum size of the buffer pool
     * @param dm the disk manager to call to read/write pages
     */
    public BufferManagerImpl(int numPages, DiskManager dm) {
        diskManager = dm;
        this.numPages = numPages;
        frames = new HashMap<>(numPages);
        lru = new PriorityBlockingQueue<>(numPages, frameComparator);
    }


    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {
        Frame frame;
        if(frames.containsKey(pid)){
            frame = frames.get(pid);
            frame.pinCount ++;
            lru.remove(frame);
            return frame.page;
        }
        else if(frames.size() == numPages){
            if(!evictPage()){
                throw new BufferManagerException("The buffer pool is full and cannot find a page to evict");
            }
        }
        Page p = diskManager.readPage(pid, pageMaker);
        frame = new Frame(p);
        frames.put(pid,frame);
        return p;

    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {
        if(!frames.containsKey(pid)){
            throw new BufferManagerException("pid is not in the cache!");
        }
        Frame temp = frames.get(pid);
        if(temp.pinCount == 0){
            throw new BufferManagerException("pin count is already zero!");
        }
        temp.pinCount --;
        if(!temp.isDirty){
            temp.isDirty = isDirty;
        }
        if(temp.pinCount == 0 && ((!temp.isDirty) || allowEvictDirty)){
            lru.add(temp);
        }
    }

    @Override
    public synchronized void flushPage(PageId pid) {
        Frame temp = frames.get(pid);
        if(temp.isDirty){
            diskManager.writePage(temp.page);
            temp.isDirty = false;
            if(!allowEvictDirty && temp.pinCount == 0){
                lru.add(temp);
            }
        }
    }

    @Override
    public synchronized void flushAllPages() {
        for(Frame frame: frames.values()){
            if(frame.isDirty){
                diskManager.writePage(frame.page);
                frame.isDirty = false;
                if(!allowEvictDirty && frame.pinCount == 0){
                    lru.add(frame);
                }
            }
        }
    }

    @Override
    public synchronized void evictDirty(boolean allowEvictDirty) {
        this.allowEvictDirty = allowEvictDirty;
    }

    @Override
    public synchronized void allocatePage(PageId pid) {
        diskManager.allocatePage(pid);
    }

    @Override
    public synchronized boolean isDirty(PageId pid) {
        if(!frames.containsKey(pid)){
            return false;
        }
        return frames.get(pid).isDirty;
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        return frames.containsKey(pid);
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        if(!inBufferPool(pid)){
            throw new BufferManagerException("the page is not in cache");
        }
        return frames.get(pid).page;
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        frames.remove(pid);
    }


    /**
     * The code uses a priority queue to save frames that can be evicted. Only in unpin method that the pin_count of a
     * frame can drop to 0. As a result, the code will add the specific frame to the queue during the unpin phase. In
     * method evictdirty, the code will just poll a frame from the queue and removes it from the buffer. The page eviction
     * and pinpage methods do not depend on the size of the buffer pool.
     *
     * If the boolean value of allowEvictDirty is set to false, dirty frame will not be added to the queue. However, after
     * the frame is flushed, it will be added to the queue.
     */
    public synchronized boolean evictPage(){
        Frame temp = lru.poll();
        if(temp == null){
            return false;
        }
        PageId pid = temp.page.getId();
        flushPage(pid);
        frames.remove(pid);
        return true;
    }

    public static Comparator<Frame> frameComparator = new Comparator<Frame>(){

        @Override
        public int compare(Frame f1, Frame f2) {
            return (int) (f1.pinCount - f2.pinCount);
        }
    };


    /**
     * A frame holds one page and maintains state about that page.  You are encouraged to use this
     * in your design of a BufferManager.  You may also make any warranted modifications.
     */
    private class Frame {
        private Page page;
        private int pinCount;
        public boolean isDirty;

        public Frame(Page page) {
            this.page = page;
            this.pinCount = 1;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
        }
    }

}