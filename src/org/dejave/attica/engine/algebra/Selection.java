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
 * Selection: The algebraic representation of a selection.
 *
 * @author sviglas
 */
public class Selection extends AlgebraicOperator {
	
    /** This selection's qualification. */
    private Qualification qualification;
    
    /**
     * Default constructor.
     */
    public Selection() {
        this(null);
    } // Selection()

    
    /**
     * Constructs a new algebraic selection operator.
     * 
     * @param qualification the qualification of this selection.
     */
    public Selection(Qualification qualification) {
        this.qualification = qualification;
    } // Selection()

    
    /**
     * Returns this selection's qualification.
     * 
     * @return the qualification of this selection.
     */
    public Qualification getQualification() {
        return qualification;
    } // getQualification()

    
    /**
     * A textual representation of this selection.
     * 
     * @return this selection's textual representation.
     */
    @Override
    public String toString() {
        return "[select (" + qualification.toString() + ")]";
    } // toString()

} // Selection
