/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequenct modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.List;
import java.util.ArrayList;

/**
 * BinaryOperator: The basic binary operator class.
 *
 * @author sviglas
 */
public abstract class BinaryOperator extends Operator {

    /** Denotes the left-hand side input operator. */
    // no need for an enumeration, this simply just references the
    // 0'th input of the operator
    public static final int LEFT = 0;
    
    /** Denotes the right-hand side input operator. */
    public static final int RIGHT = 1;

    
    /**
     * Constructs a new binary operator given a left and a right
     * input.
     * 
     * @param leftInput the left input operator to this binary
     * operator.
     * @param rightInput the right input operator to this binary
     * operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
    public BinaryOperator(Operator leftInput, Operator rightInput) 
	throws EngineException {

	super();
        List<Operator> inops = new ArrayList<Operator>();
        inops.add(leftInput);
        inops.add(rightInput);
        setInputs(inops);
    } // BinaryOperator()
    
} // BinaryOperator
