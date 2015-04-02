/*
 * Created on Dec 13, 2003 by sviglas
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

/**
 * SourceOperator: An operator that acts as a source to other
 * operators.
 *
 * @author sviglas
 */
public abstract class SourceOperator extends Operator {
	
    /**
     * Constructs a new source operator.
     */
    public SourceOperator() throws EngineException {
        super();
    } // SourceOperator()

} // SourceOperator
