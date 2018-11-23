package colgatedb.transactions;

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

    // store pid, tableentry pair
    private HashMap<PageId,LockTableEntry> map;

    // wairgraph object for deadlock detection
    private WaitGraph waitgraph;

    public LockManagerImpl() {
        map = new HashMap<>();
        waitgraph = new WaitGraph();
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
                waitgraph.addTid(tid);
                waitgraph.resetGraph();
                LockTableEntry tableentry = map.get(pid);
                waitgraph.addWaitTid(tid, tableentry.getLockHolders());
                // check if there is a circle
                if(waitgraph.deadLockDetection(tid)){
                    // implement wait-die policy for deadlock
                    if(!waitgraph.checkOrder(tid)){
                        throw new TransactionAbortedException();
                    }
                }
                LockTableEntry.LockRequest lockrequest =  tableentry.addRequests(tid,perm);
                // if the txn got the lock, then it should not wait
                if(tableentry.acquireLock(lockrequest)){
                    waitgraph.removeTidFromAllWaitForGraph(tid);
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
        // if this txn has no lock on the page, is should return false
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
        map.get(pid).releaseLock(tid);
        waitgraph.removeTidFromAllWaitForGraph(tid);
        notifyAll();
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        ArrayList<PageId> lockedpage = new ArrayList<>();
        for(PageId pid: map.keySet()){
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

    /**
     * An inner class for implementing the wait-for graph for deadlock detection
     */
    private class WaitGraph{

        // mapping between a tid and other tids this one is waiting for
        private HashMap<TransactionId, HashSet<TransactionId>> graph;

        // mapping between a tid and its mark used in DFS
        private HashMap<TransactionId, String> mark;

        // The arrival time of each transaction
        private List<TransactionId> order;

        private WaitGraph(){
            graph = new HashMap<>();
            order = new ArrayList<>();
            mark = new HashMap<>();
        }

        /**
         * Add a tid to the graph
         * @param tid
         */
        private void addTid(TransactionId tid){
            if(!graph.containsKey(tid)){
                graph.put(tid, new HashSet<>());
                mark.put(tid, "No");
                order.add(tid);
            }
        }

        /**
         * Add all tids that a specific tid waits
         * @param tid
         * @param tids a list of tids
         */
        private void addWaitTid(TransactionId tid, List<TransactionId> tids){
            HashSet<TransactionId> wait = graph.get(tid);
            wait.addAll(tids);
            wait.remove(tid);
        }

        /**
         * Remove this tid from all waitfor list
         * @param tid
         */
        private void removeTidFromAllWaitForGraph(TransactionId tid){
            for(TransactionId alltid: graph.keySet()){
                graph.get(alltid).remove(tid);
            }
        }

        /**
         * Reset the graph for next time usage
         */
        private void resetGraph(){
            mark.replaceAll((k,v) -> "No");
        }

        /**
         * Check if any holder is order than the requestor
         * @param tid
         * @return true if this requestor is the earliest one.
         */
        private boolean checkOrder(TransactionId tid){
            for(TransactionId lockholder: graph.get(tid)){
                if(order.indexOf(tid) > order.indexOf(lockholder)){
                    return false;
                }
            }
            return true;
        }

        /**
         * Check if there is a circle in the wait-for graph by using DFS
         * @param tid
         * @return true if there is a circle
         */
        private synchronized boolean deadLockDetection(TransactionId tid){
            // if this tid has already been marked as finished, that means there is a circle
            if(mark.get(tid).equals("Finished")){
                return true;
            }
            // reach a leaf
            if(graph.get(tid).isEmpty()){
                return false;
            }
            mark.put(tid, "Searching");
            for(TransactionId child: graph.get(tid)){
                // if the child is searching its child, then there is a circle
                if(mark.get(child).equals("Searching")){
                    return true;
                }
                // if the child has a circle, then there is a circle
                if(deadLockDetection(child)){
                    return true;
                }
            }
            mark.put(tid, "Finished");
            return false;
        }
    }

}
