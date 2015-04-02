/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.predicates;

/**
 * PredicateEvaluator: The predicate evaluator for the entire system.
 *
 * @author sviglas
 */
public class PredicateEvaluator {
	
    /**
     * Convenience method to evaluate predicates.
     * 
     * @param pred the predicate to be evaluated.
     * @return <code>true</code> if the predicate holds,
     * <code>false</code> if it does not.
     */
    public static boolean evaluate(Predicate pred) {
        return pred.evaluate();
    } // evaluate()

} // PredicateEvaluator
