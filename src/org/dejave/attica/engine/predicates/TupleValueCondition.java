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
 * TupleValueCondition: A condition on a tuple slot and a value.
 *
 * @author sviglas
 */
public class TupleValueCondition implements Predicate {
	
    /** The pointer to the tuple slot used by the predicate. */
    private TupleSlotPointer leftSlot;
	
    /** The tuple the predicate is to be evaluated on. */
    private Tuple leftTuple;
    
    /** The <code>Comparable</code> right-hand side value. */
    private Comparable rightValue;
    
    /** The qualification between the tuple slot and the value. */
    private Condition.Qualification qualification;

    
    /**
     * Constructs a new <code>TupleValueCondition</code> given a pointer to a 
     * tuple slot and a <code>Comparable</code>.
     * 
     * @param leftSlot the pointer to the tuple slot
     * @param right the Java <code>Comparable</code> value used for
     * comparison.
     * @param qualification the qualification between the value of the
     * appropriate slot of the tuple and the <code>Comparable</code>
     * value.
     */
    public TupleValueCondition(TupleSlotPointer leftSlot,
                               Comparable right,
                               Condition.Qualification qualification) {
        this.leftSlot = leftSlot;
        rightValue = right;
        this.qualification = qualification;
    } // TupleValueCondition()

    
    /**
     * Sets the tuple this predicate is to be evaluated on.
     * 
     * @param leftTuple the tuple the predicate is to be evaluated on.
     */
    public void setTuple(Tuple leftTuple) {
        this.leftTuple = leftTuple;
    } // setTuple()

    
    /**
     * Implements the <code>Predicate</code> interface by defining
     * the </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */
    public boolean evaluate() {
        Comparable left = 
            (Comparable) leftTuple.getValue(leftSlot.getSlot());
        return PredicateEvaluator.evaluate(new Condition(left,
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
        return leftSlot + symbolString() + rightValue;
    } // toString()
	
	/**
	 * Convert the qualification to a string
	 * 
	 * @return the symbol of the qualification as a string
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

} // TupleValueCondition
