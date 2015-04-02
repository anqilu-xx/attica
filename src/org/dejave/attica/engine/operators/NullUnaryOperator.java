/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Jan 26, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import org.dejave.attica.engine.predicates.TrueCondition;

/**
 * @author sviglas
 *
 * NullUnaryOperator: The null unary operator -- 
 * a select with a true predicate
 */
public class NullUnaryOperator extends Select {
	
    /**
     * Constructs a new null operator.
     * 
     * @param operator the incoming operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly initialised.
     */
    public NullUnaryOperator(Operator operator) throws EngineException {
        super(operator, new TrueCondition());
    } // NullUnaryOperator()

} // NullUnaryOperator
