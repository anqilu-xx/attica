/*
 * Created on Dec 11, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.predicates;

import org.dejave.attica.storage.Tuple;

/**
 * PredicateTupleInserter: It inserts one or more tuples into a
 * predicate depending on whether the predicate is a tuple/value or a
 * tuple/tuple predicate.
 *
 * @author sviglas
 */
public class PredicateTupleInserter {
	
    /**
     * Inserts only a single tuple in the given predicate.
     * 
     * @param leftTuple the tuple to be inserted.
     * @param predicate the predicate to be modified.
     */
    public static void insertTuple(Tuple leftTuple,
                                   Predicate predicate) {
        insertTuples(leftTuple, null, predicate);
    } // insertTuple
    
    
    /**
     * Inserts two tuples in the given predicate.
     * 
     * @param leftTuple the left tuple of the predicate.
     * @param rightTuple the right tuple of the predicate.
     * @param predicate the predicate to be modified.
     */
    public static void insertTuples(Tuple leftTuple,
                                    Tuple rightTuple,
                                    Predicate predicate) {
        if (predicate instanceof TupleValueCondition) {
            // this is a tuple/value predicate -- set only the left tuple
            TupleValueCondition tvc = (TupleValueCondition) predicate;
            tvc.setTuple(leftTuple);
        }
        else if (predicate instanceof TupleTupleCondition) {
            // this is a tuple/tuple predicate -- set both tuples
            TupleTupleCondition ttc = (TupleTupleCondition) predicate;
            ttc.setTuples(leftTuple, rightTuple);
        }
        else if (predicate instanceof ListPredicate) {
            // this is a list of predicates -- recurse into the list
            // setting the tuples
            ListPredicate lp = (ListPredicate) predicate;
            for (Predicate pred : lp.predicates())
                insertTuples(leftTuple, rightTuple, pred);
        }
        else if (predicate instanceof Negation) {
            // this is a negation -- modify the negated predicate
            Negation n = (Negation) predicate;
            Predicate p = n.getPredicate();
            insertTuples(leftTuple, rightTuple, p);
            n.setPredicate(p);
        }
    } // insertTuples()
    
} // PredicateTupleInserter
