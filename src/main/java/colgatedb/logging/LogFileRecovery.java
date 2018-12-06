package colgatedb.logging;

import colgatedb.Database;
import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.transactions.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

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
public class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     * <p>
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        System.out.println("-------------- PRINT OF LOG FILE -------------- ");
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    Page afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() + ">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() + ">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     * <p>
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
        readOnlyLog.seek(readOnlyLog.length() - LogFileImpl.LONG_SIZE);
        while (readOnlyLog.getFilePointer() > 0) {
            long begin = readOnlyLog.readLong();
            readOnlyLog.seek(begin);
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            if (tid == tidToRollback.getId()) {
                if (type == LogType.UPDATE_RECORD) {
                    Page page = LogFileImpl.readPageData(readOnlyLog);
                    Database.getBufferManager().discardPage(page.getId());
                    Database.getDiskManager().writePage(page);
                    Database.getLogFile().logCLR(tid,page);
                }
                else if (type == LogType.BEGIN_RECORD) {
                    Database.getLogFile().logAbort(tidToRollback.getId());
                }
                else if (type == LogType.COMMIT_RECORD){
                    throw new IOException();
                }
            }
            readOnlyLog.seek(begin - LogFileImpl.LONG_SIZE);
        }

    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     * <p>
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
        readOnlyLog.seek(0);
        long checkpoint = readOnlyLog.readLong();
        Set<Long> losers = new HashSet<>();
        print();
        if(checkpoint == -1){
            System.out.println("are you here");
        }
        else{
            //System.out.println(checkpoint);
            readOnlyLog.seek(checkpoint);
            //System.out.println(readOnlyLog.getFilePointer());
            readOnlyLog.readInt();
            readOnlyLog.readLong();
            int activetids = readOnlyLog.readInt();
            //System.out.println(activetids);
            for (int i = 0; i < activetids; i++) {
                long tid = readOnlyLog.readLong();
                System.out.println(tid);
                losers.add(tid);
            }
            readOnlyLog.readLong();
            //System.out.println(readOnlyLog.getFilePointer());
            //System.out.println(readOnlyLog.length());
        }
        //System.out.println(readOnlyLog.getFilePointer());
        //System.out.println(readOnlyLog.length());
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            //System.out.println(readOnlyLog.getFilePointer());
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    losers.add(tid);
                    break;
                case LogType.COMMIT_RECORD:
                    losers.remove(tid);
                    break;
                case LogType.ABORT_RECORD:
                    losers.remove(tid);
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeimage = LogFileImpl.readPageData(readOnlyLog);
                    Page afterimage = LogFileImpl.readPageData(readOnlyLog);
                    Database.getDiskManager().writePage(afterimage);
                    break;
                case LogType.CLR_RECORD:
                    afterimage = LogFileImpl.readPageData(readOnlyLog);
                    Database.getDiskManager().writePage(afterimage);
                    break;
            }
            long startOfRecord = readOnlyLog.readLong();
            //System.out.println(readOnlyLog.getFilePointer());
        }

        // Undo:

        readOnlyLog.seek(readOnlyLog.length() - LogFileImpl.LONG_SIZE);
        while (readOnlyLog.getFilePointer() > 0) {
            long begin = readOnlyLog.readLong();
            readOnlyLog.seek(begin);
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            if (losers.contains(tid)) {
                if (type == LogType.UPDATE_RECORD) {
                    Page page = LogFileImpl.readPageData(readOnlyLog);
                    Database.getDiskManager().writePage(page);
                    //print();
                    Database.getLogFile().logCLR(tid,page);
                    //print();
                }
                else if (type == LogType.BEGIN_RECORD) {
                    Database.getLogFile().logAbort(tid);
                    losers.remove(tid);
                    if(losers.isEmpty()){
                        break;
                    }
                }
            }
            readOnlyLog.seek(begin - LogFileImpl.LONG_SIZE);
        }

    }


}
