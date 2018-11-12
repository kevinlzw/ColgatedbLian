package colgatedb.transactions;

import colgatedb.page.SimplePageId;
import com.gradescope.jh61b.grader.GradedTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

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
public class LockScheduleTest {
    private TransactionId tid0 = new TransactionId();
    private TransactionId tid1 = new TransactionId();
    private TransactionId tid2 = new TransactionId();
    private TransactionId tid3 = new TransactionId();
    private SimplePageId pid1 = new SimplePageId(0, 1);
    private SimplePageId pid2 = new SimplePageId(0, 2);
    private SimplePageId pid3 = new SimplePageId(0, 3);
    private SimplePageId pid4 = new SimplePageId(0, 4);
    private LockManager lm;
    private Schedule.Step[] steps;
    private Schedule schedule;

    @Before
    public void setUp() {
        lm = new LockManagerImpl();
    }

    @Test
    @GradedTest(number="19.1", max_score=1.0, visibility="visible")
    public void acquireLock() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),
                // important detail: acquired step must be included in schedule and should appear as soon as the
                // lock is acquired.  in this case, the lock is acquired immediately.
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED)
        };
        executeSchedule();
    }

    /**
     * Tricky test case:
     * - T1 has shared lock and T2 waiting on exclusive
     * - then T1 requests upgrade, it should be granted because upgrades get highest priority
     */
    @Test
    @GradedTest(number="19.2", max_score=1.0, visibility="visible")
    public void upgradeRequestCutsInLine() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),
                new Schedule.Step(tid1, pid1, Schedule.Action.EXCLUSIVE),  // t2 waiting for exclusive
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),  // t1 requests upgrade, should be able to cut line
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // t1 gets exclusive ahead of t2
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED)    // now t2 can get exclusive
        };
        executeSchedule();
    }

    // write three unit tests here

    /**
     * Tricky test case:
     * - T1, T2 have shared lock and T1 is waiting on a upgrade to exclusive
     * - then T3 requests shared lock, T1 should be granted after T2 unlocked to prevent starvation
     */
    @Test
    @GradedTest(number="19.2", max_score=1.0, visibility="visible")
    public void upgradeRequestCutsInLine2() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),     // t2 requests shared
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),    // t2 got the lock
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),    // t1 got the lock
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),     // t1 requests upgrade
                new Schedule.Step(tid2, pid1, Schedule.Action.SHARED),     // t3 requests shared
                new Schedule.Step(tid1, pid1, Schedule.Action.UNLOCK),     // t2 unlocked
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // t1 got the lock
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),     // t1 unlocked
                new Schedule.Step(tid2, pid1, Schedule.Action.ACQUIRED)   // t3 got the lock
        };
        executeSchedule();
    }


    /**
     * Test case:
     * - T1 has shared lock on pid1, 2 and T2, T3 are waiting on exclusive locks
     * - then T1 unlocked pid1, 2,T2 and T3 should be granted after T1 unlocked
     */
    @Test
    @GradedTest(number="19.2", max_score=1.0, visibility="visible")
    public void twoDifferentPages() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared pid1
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),     // t1 got lock for pid1
                new Schedule.Step(tid0, pid2, Schedule.Action.SHARED),     // t1 requests shared pid2
                new Schedule.Step(tid0, pid2, Schedule.Action.ACQUIRED),     // t1 got lock for pid2
                new Schedule.Step(tid1, pid1, Schedule.Action.EXCLUSIVE),     // t2 requests exclusive pid1
                new Schedule.Step(tid2, pid2, Schedule.Action.EXCLUSIVE),     // t3 requests exclusive pid2
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),     // t1 unlocked pid1
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),     // t2 got lock for pid1
                new Schedule.Step(tid0, pid2, Schedule.Action.UNLOCK),     // t1 unlocked pid2
                new Schedule.Step(tid2, pid2, Schedule.Action.ACQUIRED),     // t3 got lock for pid2
        };
        executeSchedule();
    }


    /**
     * Tricky test case:
     * - T1, T2 have shared lock and T3 is waiting on an exclusive lock
     * - then T1 requests a upgrade, T1 should be granted after T2 unlocked
     */
    @Test
    @GradedTest(number="19.2", max_score=1.0, visibility="visible")
    public void upgradeRequestCutsInLine3() {
        steps = new Schedule.Step[]{
                new Schedule.Step(tid1, pid1, Schedule.Action.SHARED),     // t2 requests shared
                new Schedule.Step(tid1, pid1, Schedule.Action.ACQUIRED),    // t2 got the lock
                new Schedule.Step(tid0, pid1, Schedule.Action.SHARED),     // t1 requests shared
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),    // t1 got the lock
                new Schedule.Step(tid2, pid1, Schedule.Action.EXCLUSIVE),     // t3 requests the exclusive lock
                new Schedule.Step(tid0, pid1, Schedule.Action.EXCLUSIVE),     // t1 requests upgrade
                new Schedule.Step(tid1, pid1, Schedule.Action.UNLOCK),     // t2 unlocked
                new Schedule.Step(tid0, pid1, Schedule.Action.ACQUIRED),   // t1 got the lock
                new Schedule.Step(tid0, pid1, Schedule.Action.UNLOCK),     // t1 unlocked
                new Schedule.Step(tid2, pid1, Schedule.Action.ACQUIRED)   // t3 got the lock
        };
        executeSchedule();
    }


    private void executeSchedule() {
        try {
            schedule = new Schedule(steps, lm);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(schedule.allStepsCompleted());
    }
}
