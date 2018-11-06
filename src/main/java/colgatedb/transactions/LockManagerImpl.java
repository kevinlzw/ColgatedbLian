package colgatedb.transactions;

import colgatedb.page.Page;
import colgatedb.page.PageId;

import java.util.*;

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
public class LockManagerImpl implements LockManager {

    private HashMap<PageId,LockTableEntry> map;


    public LockManagerImpl() {
        map = new HashMap<>();
    }

    @Override
    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if(!map.containsKey(pid)){
            LockTableEntry entry = new LockTableEntry();
            map.put(pid, entry);
        }
        LockTableEntry tableentry = map.get(pid);
        if(!holdsLock(tid,pid,perm)){

        }

    }

    @Override
    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        LockTableEntry entry = map.get(pid);
        Permissions permission = entry.getLock(tid);
        if(permission == null){
            return false;
        }
        else if(permission == Permissions.READ_ONLY){
            return true;
        }
        else{
            if(perm == permission){
                return true;
            }
            else{
                return false;
            }
        }
    }

    @Override
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        throw new UnsupportedOperationException("implement me!");
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        throw new UnsupportedOperationException("implement me!");
    }
}
