/*
 * Created on Dec 14, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.predicates;

/**
 * TrueCondition: A condition that always evaluates to true.
 *
 * @author sviglas
 */
public class TrueCondition implements Predicate {
    
    /**
     * Implementation of the <code>Predicate</code> interface.
     * 
     * @return <code>true</code> by default.
     */
    public boolean evaluate() {
        return true;
    } // evaluate()

    
    /**
     * Textual representation.
     * 
     * @return textual representation.
     */
    @Override
    public String toString() {
        return "TRUE";
    } // toString()
} // TrueCondition
