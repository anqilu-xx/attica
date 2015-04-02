/*
 * Created on Dec 10, 2003 by sviglas
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

import org.dejave.attica.engine.predicates.Predicate;
import org.dejave.attica.engine.predicates.PredicateEvaluator;
import org.dejave.attica.engine.predicates.PredicateTupleInserter;

import org.dejave.attica.model.Relation;

import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.IntermediateTupleIdentifier;

/**
 * Select: Implementation of a selection operator.
 *
 * @author sviglas
 */
public class Select extends UnaryOperator {
	
    /** The predicate this selection operator evaluates. */
    private Predicate predicate;

    /** Reusable return list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new selection operator given its input.
     * 
     * @param operator the input operator.
     * @param predicate the predicate this selection operator
     * evaluates.
     * @throws EngineException thrown whenever the selection operator cannot
     * be properly constructed.
     */
    public Select (Operator operator, Predicate predicate) 
	throws EngineException {
        
        super(operator);
        this.predicate = predicate;
        returnList = new ArrayList<Tuple>();
    } // Select()

    
    /**
     * Processes an incoming tuple.
     * 
     * @param tuple the tuple to be processed. 
     * @param inOp the incoming operator this tuple belongs to (should
     * always default to '0').
     * @throws EngineException thrown whenever the tuple cannot be
     * evaluated
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        // call the tuple inserter to insert the tuple into the predicate
        // for proper evaluation
        PredicateTupleInserter.insertTuple(tuple, predicate);
        boolean value = PredicateEvaluator.evaluate(predicate);
        returnList.clear();
        if (value) {
            Tuple out =
                new Tuple(new IntermediateTupleIdentifier(tupleCounter++),
                          tuple.getValues());
            returnList.add(out);
        }
        return returnList;
    } // innerProcessTuple()

    
    /**
     * Return a new relation for this operator's output relation.
     * 
     * @return the output relation of this operator.
     */
    @Override
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // SetOutputRelation()

    
    /**
     * Textual representation.
     */
    @Override
    protected String toStringSingle () {
        return "select <" + predicate + ">";
    } // toStringSingle()

} // Select
