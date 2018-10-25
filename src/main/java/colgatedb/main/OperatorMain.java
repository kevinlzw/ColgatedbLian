package colgatedb.main;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.operators.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Op;
import colgatedb.tuple.StringField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
public class OperatorMain {

    public static void main(String[] argv)
            throws DbException, TransactionAbortedException, IOException {

        // file named college.schema must be in colgatedb directory
        String filename = "college.schema";
        System.out.println("Loading schema from file: " + filename);
        Database.getCatalog().loadSchema(filename);

        /* SELECT S.name
        FROM Students S, Takes T, Profs P
        WHERE S.sid = T.sid AND
        T.cid = P.favoriteCourse AND
        P.name = "hay"
        */
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=hay
        // - a Join operator that joins the Profs table with Takes
        // - a Join operator that joins the result table with Students
        // - a Project operator that projects the field out
        TransactionId tid = new TransactionId();
        SeqScan scanStudents = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        SeqScan scanTakes = new SeqScan(tid, Database.getCatalog().getTableId("Takes"));
        SeqScan scanProfs = new SeqScan(tid, Database.getCatalog().getTableId("Profs"));
        StringField hay = new StringField("hay", Type.STRING_LEN);
        Predicate p = new Predicate(1, Op.EQUALS, hay);
        DbIterator filterresult = new Filter(p, scanProfs);
        JoinPredicate jp1 = new JoinPredicate(2, Op.EQUALS,1);
        filterresult = new Join(jp1,filterresult,scanTakes);
        JoinPredicate jp2 = new JoinPredicate(3, Op.EQUALS,0);
        filterresult = new Join(jp2,filterresult,scanStudents);
        ArrayList<Integer> filedname = new ArrayList<Integer>();
        filedname.add(6);
        ArrayList<Type> filedtype = new ArrayList<Type>();
        filedtype.add(Type.STRING_TYPE);
        filterresult = new Project(filedname,filedtype,filterresult);



        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        filterresult.open();
        while (filterresult.hasNext()) {
            Tuple tup = filterresult.next();
            System.out.println("\t"+tup);
        }
        filterresult.close();
    }

}