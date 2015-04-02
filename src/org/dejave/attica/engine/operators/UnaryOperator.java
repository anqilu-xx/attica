/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.List;
import java.util.ArrayList;

/**
 * UnaryOperator: Models a unary engine operator.
 *
 * @author sviglas
 */
public abstract class UnaryOperator extends Operator {

    /**
     * Constructs a new unary operator given an input operator.
     * 
     * @throws EngineException thrown whenever the unary operator
     * cannot be properly constructed.
     */
    public UnaryOperator() throws EngineException {
        
        super();
        List<Operator> inops = new ArrayList<Operator>();
        setInputs(inops);
    } // UnaryOperator()
    
    
    /**
     * Constructs a new unary operator given an input operator.
     * 
     * @param operator the input operator of this unary operator.
     * @throws EngineException thrown whenever the unary operator
     * cannot be properly constructed.
     */
    public UnaryOperator(Operator operator) throws EngineException {
        
        super();
        List<Operator> inops = new ArrayList<Operator>();
        inops.add(operator);
        setInputs(inops);
    } // UnaryOperator()


    /**
     * Returns the sole input operator of this unary operator.
     * 
     * @return this unary operator's sole input operator.
     * @throws EngineException ideally it should never be thrown.
     */
    public Operator getInputOperator() throws EngineException {
        return getInputOperator(0);
    } // getInputOperator()
	
} // UnaryOperator
