/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.algebra;

/**
 * Join: An algebraic join operator.
 *
 * @author sviglas
 */
public class Join extends AlgebraicOperator {
	
    /** This join's qualification. */
    private Qualification qualification;
	
    /** Default constructor. */
    public Join() {
        this(null);
    } // Join()

    
    /**
     * Constructs a new algebraic join operator.
     * 
     * @param qualification the qualification of this join.
     */
    public Join(Qualification qualification) {
        this.qualification = qualification;
    } // Join()

    
    /**
     * Return this join's qualification.
     * 
     * @return the qualification of this join.
     */
    public Qualification getQualification() {
        return qualification;
    } // getQualification()

    
    /**
     * A textual representation of this join.
     * 
     * @return this join's textual representation.
     */
    @Override
    public String toString() {
        return "[join (" + qualification.toString() + ")]";
    } // toString()
} // Join
