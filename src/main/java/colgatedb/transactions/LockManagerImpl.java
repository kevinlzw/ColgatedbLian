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

    private HashMap<TransactionId, Node> nodes;

    private int tidorder;

    public LockManagerImpl() {
        map = new HashMap<>();
        nodes = new HashMap<>();
        tidorder = 0;
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        boolean waiting = true;
        while (waiting) {
            synchronized (this) {
                if (!map.containsKey(pid)) {
                    LockTableEntry entry = new LockTableEntry();
                    map.put(pid, entry);
                }
                if(!nodes.containsKey(tid)){
                    tidorder ++;
                    Node node = new Node(tidorder);
                    nodes.put(tid,node);
                }
                LockTableEntry tableentry = map.get(pid);
                nodes.get(tid).waitfor.addAll(tableentry.getLockHolders());
                nodes.get(tid).waitfor.remove(tid);
                for(TransactionId alltid: nodes.keySet()){
                    Node temp = nodes.get(alltid);
                    temp.ifexisted = "no";
                }
                if(deadLockDetection(tid)){ 
                    Node currenttid = nodes.get(tid);
                    for(TransactionId lockholder: tableentry.getLockHolders()){
                        Node node = nodes.get(lockholder);
                        if(currenttid.myorder > node.myorder){
                            throw new TransactionAbortedException();
                        }
                    }
                }
                LockTableEntry.LockRequest lockrequest =  tableentry.addRequests(tid,perm);
                if(tableentry.acquireLock(lockrequest, tid, nodes.get(tid))){
                    waiting = false;
                    for(TransactionId alltid: nodes.keySet()){
                        nodes.get(alltid).waitfor.remove(tid);
                    }
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
            return perm == permission;
        }
    }

    @Override
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if(!holdsLock(tid,pid,Permissions.READ_ONLY)){
            throw new LockManagerException("This transaction does not hold any locks!");
        }
        LockTableEntry entry = map.get(pid);
        entry.releaseLock(tid);
        for(TransactionId alltid: nodes.keySet()){
            nodes.get(alltid).waitfor.remove(tid);
        }
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

    public synchronized boolean deadLockDetection(TransactionId tid){
        Node node = nodes.get(tid);
        if(node.ifexisted.equals("finished")){
            return true;
        }
        if(node.waitfor.isEmpty()){
            return false;
        }
        node.ifexisted = "searching";
        for(TransactionId child: node.waitfor){
            Node childnode = nodes.get(child);
            if(childnode.ifexisted.equals("searching")){
                return true;
            }
            if(deadLockDetection(child)){
                return true;
            }
        }
        node.ifexisted = "finished";
        return false;
    }



    public class Node{

        public int myorder;

        public String ifexisted;

        public HashSet<TransactionId> waitfor;

        public Node(int tidorder){
            waitfor = new HashSet<>();
            ifexisted = "no";
            myorder = tidorder;
        }
    }




}
