/*
 * Created on Dec 18, 2003 by sviglas
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

import org.dejave.attica.model.Attribute;
import org.dejave.attica.model.Relation;

import org.dejave.attica.engine.predicates.Predicate;

import org.dejave.attica.storage.StorageManager;

/**
 * PhysicalJoin: The base class for all joins.  Contains some
 * additional "plumbing" for join implementation.
 *
 * @author sviglas
 */
public abstract class PhysicalJoin extends BinaryOperator {
    
    /** The storage manager for this operator. */
    private StorageManager sm;
	
    /** The predicate evaluated by this join operator. */
    private Predicate predicate;

	
    /**
     * Constructs a new physical join operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator
     * @param sm the storage manager.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be 
     * properly constructed.
     */
    public PhysicalJoin(Operator left, Operator right,
                        StorageManager sm, Predicate predicate) 
	throws EngineException {
        
        super(left, right);
        this.sm = sm;
        this.predicate = predicate;
    } // PhysicalJoin()


    /**
     * Retrieves the storage manager of this physical join.
     *
     * @return the storage manager.
     */
    protected StorageManager getStorageManager() {
        return sm;
    } // getStorageManager()


    /**
     * Retrieves the predicate of this physical join.
     *
     * @return the predicate.
     */
    protected Predicate getPredicate () {
        return predicate;
    } // getPredicate()

    
    /**
     * Sets the output relation for this operator.
     * 
     * @return the output relation for this operator.
     * @throws EngineException thrown whenever the output relation cannot
     * be constructed.
     */
    protected Relation setOutputRelation() throws EngineException {
        List<Attribute> attributes = new ArrayList<Attribute>();
        Relation rel = getInputOperator(LEFT).getOutputRelation();
        for (int i = 0; i < rel.getNumberOfAttributes(); i++)
            attributes.add(rel.getAttribute(i));
    
        rel = getInputOperator(RIGHT).getOutputRelation();
        for (int i = 0; i < rel.getNumberOfAttributes(); i++)
            attributes.add(rel.getAttribute(i));
		
        return new Relation(attributes);
    } // setOutputRelation()

} // PhysicalJoin
