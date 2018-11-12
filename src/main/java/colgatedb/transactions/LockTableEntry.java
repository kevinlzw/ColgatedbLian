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
    private int exclusiveinqueue;

    public LockTableEntry() {
        lockType = null;
        lockHolders = new HashSet<>();
        requests = new LinkedList<>();
        // you may wish to add statements here.
        exclusiveinqueue = 0;
    }


    public List<TransactionId> getLockHolders(){
        List<TransactionId> list = new ArrayList<>();
        list.addAll(lockHolders);
        return list;
    }


    public LockRequest addRequests(TransactionId tid, Permissions lockType){
        LockRequest lockrequest = new LockRequest(tid, lockType);
        if(lockHolders.contains(tid) && this.lockType == lockType){
            throw new LockManagerException("You have already acquired the lock!");
        }
        if(requests.contains(lockrequest)){
            return lockrequest;
        }
        if(lockType == Permissions.READ_WRITE){
            exclusiveinqueue ++;
        }
        if(lockHolders.contains(tid) && lockType == Permissions.READ_WRITE){
            requests.addFirst(lockrequest);
            return lockrequest;
        }
        requests.add(lockrequest);
        return lockrequest;
    }

    public boolean acquireLock(LockRequest lockrequest, TransactionId tid){
        if(lockrequest.perm == Permissions.READ_ONLY){
            if(requests.contains(lockrequest) && lockType != Permissions.READ_WRITE && exclusiveinqueue == 0){
                this.lockType = Permissions.READ_ONLY;
                lockHolders.add(tid);
                requests.remove(lockrequest);
                return true;
            }
            else{
                return false;
            }
        }
        // lockrequest.perm == Permissions.READ_WRITE
        else{
            if(requests.getFirst().equals(lockrequest) && (lockType == null || (lockHolders.size() == 1 && lockHolders.contains(tid)))){
                this.lockType = Permissions.READ_WRITE;
                lockHolders.add(tid);
                requests.removeFirst();
                return true;
            }
            else{
                return false;
            }
        }
    }

    public void releaseLock(TransactionId tid){
        if(lockType == Permissions.READ_WRITE && lockHolders.contains(tid)){
            exclusiveinqueue --;
        }
        lockHolders.remove(tid);
        if(lockHolders.size() == 0){
            lockType = null;
        }
    }

    public boolean ifEmpty(){
        return requests.size() == 0;
    }

    public boolean ifNoLock(){
        return lockType == null;
    }

    public Permissions getLock(TransactionId tid){
        if(lockHolders.contains(tid)){
            return lockType;
        }
        else{
            return null;
        }
    }



    // you may wish to implement methods here.

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
