package colgatedb.transactions;

import java.security.Permission;
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

/**
 * Represents the state associated with the lock on a particular page.
 */
public class LockTableEntry {

    // some suggested private instance variables; feel free to modify
    private Permissions lockType;             // null if no one currently has a lock
    private Set<TransactionId> lockHolders;   // a set of txns currently holding a lock on this page
    private Deque<LockRequest> requests;       // a queue of outstanding requests
    private int exclusiveinqueue;              // exclusive requests in the queue

    public LockTableEntry() {
        lockType = null;
        lockHolders = new HashSet<>();
        requests = new LinkedList<>();
        exclusiveinqueue = 0;
    }

    /**
     * return the lockholder as a list
     * @return lockholder as a list
     */
    public List<TransactionId> getLockHolders(){
        List<TransactionId> list = new ArrayList<>();
        list.addAll(lockHolders);
        return list;
    }


    /**
     * add a request to the queue
     * @param tid the txn waiting to be added
     * @param lockType the type this txn wants
     * @return the lockquest created for this txn
     */
    public LockRequest addRequests(TransactionId tid, Permissions lockType){
        LockRequest lockrequest = new LockRequest(tid, lockType);
        if(lockHolders.contains(tid) && this.lockType == lockType){
            return null;
        }
        // if the queue has this request, do nothing and return this request
        if(requests.contains(lockrequest)){
            return lockrequest;
        }
        if(lockType == Permissions.READ_WRITE){
            exclusiveinqueue ++;
        }
        // if the request is a upgrade quest, adds to the front of the list
        if(lockHolders.contains(tid) && lockType == Permissions.READ_WRITE){
            requests.addFirst(lockrequest);
            return lockrequest;
        }
        requests.add(lockrequest);
        return lockrequest;
    }

    /**
     * try to acquire a lock for a txn, if succeed, acquire the lock and return true
     * @param lockrequest
     * @return true if the txn got the lock, false otherwise
     */
    public boolean acquireLock(LockRequest lockrequest){
        if(lockrequest == null){
            return true;
        }
        if(lockrequest.perm == Permissions.READ_ONLY){
            // if the current locktype is not exclusive and no exclusive lockquest is waiting, then we can
            // grant the lock to this request
            if(requests.contains(lockrequest) && lockType != Permissions.READ_WRITE && exclusiveinqueue == 0){
                this.lockType = Permissions.READ_ONLY;
                lockHolders.add(lockrequest.tid);
                requests.remove(lockrequest);
                return true;
            }
            else{
                return false;
            }
        }
        // lockrequest.perm == Permissions.READ_WRITE
        else{
            // if the current locktype is null or only the txn itself is holding the shared lock, then we
            // can grant the exclusive lock
            if(requests.getFirst().equals(lockrequest) && (lockType == null || (lockHolders.size() == 1 && lockHolders.contains(lockrequest.tid)))){
                this.lockType = Permissions.READ_WRITE;
                lockHolders.add(lockrequest.tid);
                requests.removeFirst();
                exclusiveinqueue --;
                return true;
            }
            else{
                return false;
            }
        }
    }

    /**
     * release the lock for a specific tid
     * @param tid
     */
    public void releaseLock(TransactionId tid){
        lockHolders.remove(tid);
        if(lockHolders.size() == 0){
            lockType = null;
        }
    }

    /**
     * return the locktype for a txn. null if this txn does not hold any lock
     * @param tid
     * @return
     */
    public Permissions getLock(TransactionId tid){
        return lockHolders.contains(tid) ? lockType : null;
    }


    /**
     * A class representing a single lock request.  Simply tracks the txn and the desired lock type.
     * Feel free to use this, modify it, or not use it at all.
     */
    public class LockRequest {
        public final TransactionId tid;
        public final Permissions perm;

        public LockRequest(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        public boolean equals(Object o) {
            if (!(o instanceof LockRequest)) {
                return false;
            }
            LockRequest otherLockRequest = (LockRequest) o;
            return tid.equals(otherLockRequest.tid) && perm.equals(otherLockRequest.perm);
        }

        public String toString() {
            return "Request[" + tid + "," + perm + "]";
        }
    }
}
