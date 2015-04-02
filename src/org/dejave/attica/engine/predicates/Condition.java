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
 * Condition: A basic condition between two values.
 *
 * @author sviglas
 */
public class Condition implements Predicate {

    public enum Qualification {
        EQUALS, NOT_EQUALS, GREATER, LESS, GREATER_EQUALS, LESS_EQUALS
    }
	
    /** A comparable left-hand side value. */
    private Comparable left;
	
    /** A comparable right-hand side value. */
    private Comparable right;
	
    /** The qualification between the two values. */
    private Qualification qualification;
    
    /**
     * Constructs a new condition without any arguments
     */
    public Condition() {
        this(null, null, Qualification.EQUALS);
    } // Condition() 
	

    /**
     * Constructs a new condition with two Java
     * <code>Comparable</code> objects as arguments.
     * 
     * @param left the left-hand side <code>Comparable</code> object.
     * @param right the right-hand side <code>Comparable</code>
     * object.
     * @param qualification the qualification between the two
     * <code>Comparable</code> objects.
     */
    public Condition(Comparable left, Comparable right,
                     Qualification qualification) {
        this.left = left;
        this.right = right;
        this.qualification = qualification;
    } // Condition()
	
    /**
     * Implement the <code>Predicate</code> interface by defining
     * the </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean evaluate() {
        int value = 0;
        try {
            value = left.compareTo(right);
        }
        catch (ClassCastException cce) {
            return false;
        }

        switch (qualification) {
        case EQUALS: 
            return (value == 0);
        case NOT_EQUALS: 
            return (value != 0);
        case GREATER:
            return (value > 0);
        case LESS:
            return (value < 0);
        case GREATER_EQUALS:
            return (value >= 0);
        case LESS_EQUALS:
            return (value <= 0);
        }
        
        return false;
    } // evaluate()
	
    /**
     * Textual representation.
     * 
     * @return this condition's textual representation
     */
    @Override
    public String toString () {
        return "" + left + symbolString() + right;
    } // toString()

    
    /**
     * Convert the qualification to a string.
     * 
     * @return the symbol of the qualification as a string.
     */
    protected String symbolString () {
        switch (qualification) {
        case EQUALS: 
            return "=";
        case NOT_EQUALS: 
            return "!=";
        case GREATER:
            return ">";
        case LESS:
            return "<";
        case GREATER_EQUALS:
            return ">=";
        case LESS_EQUALS:
            return "<=";
        }
	
        return "?";
    } // symbolString()
} // Condition
