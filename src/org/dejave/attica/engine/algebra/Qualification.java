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
 * Qualification: The base class of all algebraic qualifications.
 *
 * @author sviglas
 */
public class Qualification {

    public enum Relationship {
	EQUALS, NOT_EQUALS, GREATER, LESS, GREATER_EQUALS, LESS_EQUALS
    }
	
    /** The relationship between the constituents of the
	qualification. */
    private Relationship relationship;
	
    /**
     * Default constructor.
     */
    public Qualification() {
        this(Relationship.EQUALS);
    } // Qualification()
	
    /**
     * Constructs a new qualification given a relationship.
     * 
     * @param relationship this qualification's relationship.
     */
    public Qualification(Relationship relationship) {
        this.relationship = relationship;
    } // Qualification()

    
    /**
     * Returns this qualification's relationship.
     * 
     * @return the relationship of the qualification.
     */
    public Relationship getRelationship() {
        return relationship;
    } // getRelationship()

    
    /**
     * Textual representation of this quelification.
     * 
     * @return this qualification's textual representation.
     */
    @Override
    public String toString() {
        switch (relationship) {
        case EQUALS:
            return "=";
        case GREATER:
            return ">";
        case LESS:
            return "<";
        case GREATER_EQUALS:
            return ">=";
        case LESS_EQUALS:
            return "<=";
        default:
            return "!=";
        }
    } // toString()
    
} // Qualification
