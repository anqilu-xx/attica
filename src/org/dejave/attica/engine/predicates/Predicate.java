/*
 * Created on Dec 10, 2003 by sviglas
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
 * Predicate: Access to the evaluate() method -- all predicate classes
 * should implement this interface.
 *
 * @author sviglas
 */
public interface Predicate {
	
    /**
     * Method called to return true or false depending on the
     * implementation of the interface.
     * 
     * @return <code>true</code> if the predicate evaluates to 
     * <code>true</code>, <code>false</code> otherwise.
     */
    public boolean evaluate();

} // Predicate
