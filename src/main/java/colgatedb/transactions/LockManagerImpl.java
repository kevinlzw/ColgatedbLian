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
    private HashMap<TransactionId, Boolean> lockpoint;

    public LockManagerImpl() {
        map = new HashMap<>();
        lockpoint = new HashMap<>();
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if(!lockpoint.containsKey(tid)){
            lockpoint.put(tid, false);
        }
        if(lockpoint.get(tid)){
            throw new LockManagerException("You cannot acquire any new locks after you released some locks!");
        }
        boolean waiting = true;
        while (waiting) {
            synchronized (this) {
                if (!map.containsKey(pid)) {
                    LockTableEntry entry = new LockTableEntry();
                    map.put(pid, entry);
                }
                LockTableEntry tableentry = map.get(pid);
                LockTableEntry.LockRequest lockrequest =  tableentry.addRequests(tid,perm);
                if(tableentry.acquireLock(lockrequest, tid)){
                    waiting = false;
                }
                if(waiting){
                    try{
                        wait();
                    }
                    catch (InterruptedException ignored){
                    }
                }

            }
        }
    }

    @Override
    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        LockTableEntry entry = map.get(pid);
        if(entry == null){
            return false;
        }
        Permissions permission = entry.getLock(tid);
        if(permission == null){
            return false;
        }
        else if(perm == Permissions.READ_ONLY){
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
        if(!holdsLock(tid,pid,Permissions.READ_ONLY)){
            throw new LockManagerException("This transaction does not hold any locks!");
        }
        LockTableEntry entry = map.get(pid);
        entry.releaseLock(tid);
        lockpoint.put(tid, true);
        notifyAll();
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        ArrayList<PageId> lockedpage = new ArrayList<>();
        for(PageId pid: map.keySet()){
            LockTableEntry lte = map.get(pid);
            if(holdsLock(tid,pid,Permissions.READ_ONLY)){
                lockedpage.add(pid);
            }
        }
        return lockedpage;
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        LockTableEntry lte = map.get(pid);
        return lte.getLockHolders();
    }
}
