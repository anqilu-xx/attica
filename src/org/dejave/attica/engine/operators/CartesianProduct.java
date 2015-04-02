/*
 * Created on Dec 14, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import org.dejave.attica.engine.predicates.TrueCondition;

import org.dejave.attica.storage.StorageManager;

/**
 * CartesianProduct: A cartesian product between two inuput operators.
 *
 * @author sviglas
 */
public class CartesianProduct extends NestedLoopsJoin {
	
    /**
     * Constructs a new Cartesian product operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @throws EngineException thrown whenever the operator cannot be 
     * properly constructed.
     */
    public CartesianProduct(Operator left, Operator right, StorageManager sm) 
	throws EngineException {
        
        super(left, right, sm, new TrueCondition());
    } // CartesianProduct()
	
} // CartesianProduct
