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
 * Negation: The negation of a Predicate.
 *
 * @author sviglas
 */
public class Negation implements Predicate {
	
    /** The predicate to be negated. */
    private Predicate predicate;
    
    /**
     * Constructs a new negation.
     * 
     * @param predicate the predicate to be negated.
     */
    public Negation(Predicate predicate) {
        this.predicate = predicate;
    } // Negation()
	

    /**
     * Returns the predicate to be negated.
     * 
     * @return the negated predicate.
     */
    public Predicate getPredicate() {
        return predicate;
    } // getPredicate()

    
    /**
     * Sets the predicate to be negated.
     * 
     * @param predicate the new predicate to be negated.
     */
    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    } // setPredicate()

    
    /**
     * Implements the <code>Predicate</code> interface by defining the
     * </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the negation evaluates to true,
     * <code>false</code> otherwise.
     */
    public boolean evaluate() {
        return ! predicate.evaluate();
    } // Negation()


    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return "NOT (" + predicate + ")";
    } // toString()

} // Negatiion
