package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.transactions.*;

import java.nio.Buffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * ColgateDB
 *
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
public class AccessManagerImpl implements AccessManager {

    private boolean force = true;  // indicates whether force policy should be used

    private LockManager lockmanager;

    private BufferManager buffermanager;

    private Map<PageId, Map<TransactionId,Integer>> record;


    /**
     * Initialize the AccessManager, which includes creating a new LockManager.
     * @param bm buffer manager through which all page requests should be made
     */
    public AccessManagerImpl(BufferManager bm) {
        buffermanager = bm;
        record = new HashMap<>();
        lockmanager = new LockManagerImpl();
        buffermanager.evictDirty(false);
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        lockmanager.acquireLock(tid,pid,perm);
    }

    @Override
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        return lockmanager.holdsLock(tid, pid, perm);
    }

    @Override
    public void releaseLock(TransactionId tid, PageId pid) {
        lockmanager.releaseLock(tid,pid);
    }

    @Override
    public Page pinPage(TransactionId tid, PageId pid, PageMaker pageMaker) {
        synchronized (this){
            if(!record.containsKey(pid)){
                record.put(pid, new HashMap<>());
                record.get(pid).put(tid, 1);
            }
            else{
                Map<TransactionId, Integer> list = record.get(pid);
                if(list == null){
                    list = new HashMap<>();
                }
                if(!list.containsKey(tid)){
                    list.put(tid, 1);
                }
                else{
                    list.put(tid, list.get(tid) + 1);
                }
            }
            return buffermanager.pinPage(pid,pageMaker);
        }
    }

    @Override
    public void unpinPage(TransactionId tid, Page page, boolean isDirty) {
        buffermanager.unpinPage(page.getId(),isDirty);
        synchronized (this){
            Map<TransactionId, Integer> list = record.get(page.getId());
            list.put(tid, list.get(tid) - 1);
        }
    }

    @Override
    public void allocatePage(PageId pid) {
        buffermanager.allocatePage(pid);
    }

    @Override
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    @Override
    public void transactionComplete(TransactionId tid, boolean commit) {
        List<PageId> unlockpage = lockmanager.getPagesForTid(tid);
        for(int i = 0; i < unlockpage.size(); i ++){
            lockmanager.releaseLock(tid,unlockpage.get(i));
        }
        if(commit && force){
            for(PageId pid: record.keySet()){
                if(buffermanager.isDirty(pid)){
                    buffermanager.flushPage(pid);
                }
            }
        }
        else if(!commit){
            for(PageId pid: record.keySet()){
                if(record.get(pid).containsKey(tid)){
                    while(record.get(pid).get(tid) != 0){
                        buffermanager.unpinPage(pid,true);
                        record.get(pid).put(tid,record.get(pid).get(tid) - 1);
                    }
                    if(buffermanager.isDirty(pid)){
                        buffermanager.discardPage(pid);
                    }
                }
            }
        }
    }

    @Override
    public void setForce(boolean force) {
        // you do NOT need to implement this for lab10.  this will be changed in a later lab.
    }
}
