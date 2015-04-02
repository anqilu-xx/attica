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
 * VariableVariableQualification: A qualification between two
 * variables.
 *
 * @author sviglas
 */
public class VariableVariableQualification extends Qualification {
	
    /** The left-hand side variable of the qualification. */
    private Variable left;
	
    /** The right-hand side variable of the qualification. */	
    private Variable right;
	
    /**
     * Constructs a new qualification between variables.
     * 
     * @param relationship the relationship of the qualification.
     * @param left the left-hand side variable of the qualification.
     * @param right the right-hand side variable of the qualification.
     */
    public VariableVariableQualification (Relationship relationship,
                                          Variable left,
                                          Variable right) {
        super(relationship);
        this.left = left;
        this.right = right;
    } // VariableVariableQualification()

    
    /**
     * Returns the left-hand side variable.
     * 
     * @return this qualification's left-hand side variable.
     */
    public Variable getLeftVariable() {
        return left;
    } // getLeftVariable()

    
    /**
     * Returns the right-hand side variable.
     * 
     * @return this qualification's right-hand side variable.
     */
    public Variable getRightVariable() {
        return right;
    } // getRightVariable()

    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return left.toString() + " " + super.toString() + 
            " " + right.toString();
    } // toString()

} // VariableVariableQualification
