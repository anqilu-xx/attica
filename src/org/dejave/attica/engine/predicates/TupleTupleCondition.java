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

import org.dejave.attica.storage.Tuple;

/**
 * TupleTupleCondition: A condition across tuples.
 *
 * @author sviglas
 */
public class TupleTupleCondition implements Predicate {
	
    /** The pointer to the slot in the left tuple. */
    private TupleSlotPointer leftSlot;
    
    /** The left tuple. */
    private Tuple leftTuple;
	
    /** The pointer to the slot in the right tuple. */
    private TupleSlotPointer rightSlot;
	
    /** The right tuple. */
    private Tuple rightTuple;
    
    /** The qualification between the values. */
    private Condition.Qualification qualification;

    /**
     * Constructs a new condition across tuples.
     * 
     * @param leftSlot the pointer to the left-hand side tuple slot.
     * @param rightSlot the pointer to the right-hand side tuple slot.
     * @param qualification the qualification between the values of
     * the slots.
     */
    public TupleTupleCondition (TupleSlotPointer leftSlot, 
                                TupleSlotPointer rightSlot,
                                Condition.Qualification qualification) {
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        this.qualification = qualification;
    } // TupleTupleCondition()
	
    /**
     * Sets the two tuples the predicate is to be evaluated over.
     * 
     * @param leftTuple the (new) left-hand side tuple.
     * @param rightTuple the (new) right-hand side tuple.
     */
    public void setTuples(Tuple leftTuple, Tuple rightTuple) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
    } // setTuples()

    
    /**
     * Implements the <code>Predicate</code> interface by defining the
     * </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */	
    public boolean evaluate() {
        Comparable leftValue = 
            (Comparable) leftTuple.getValue(leftSlot.getSlot());
        Comparable rightValue = 
            (Comparable) rightTuple.getValue(rightSlot.getSlot());
        return PredicateEvaluator.evaluate(new Condition(leftValue,
                                                         rightValue,
                                                         qualification));
    } // evaluate()

    
    /**
     * Textual representation.
     * 
     * @return the predicate's textual representation.
     */
    @Override
    public String toString() {
        return leftSlot + symbolString() + rightSlot;
    } // toString()

    
    /**
     * Convert the qualification to a string.
     * 
     * @return the symbol of the qualification as a string.
     */
    protected String symbolString() {
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
	
} // TupleTupleCondition()
