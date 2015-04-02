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

import org.dejave.attica.model.Attribute;
import org.dejave.attica.model.Relation;

import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.IntermediateTupleIdentifier;

/**
 * Project: Implements standard attribute projection.
 *
 * @author sviglas
 */
public class Project extends UnaryOperator {
	
    /** The slots of tuples to be projected. */	
    private int [] slots;

    /** Reusable return list. */
    private List<Tuple> returnList;
    
    /**
     * Constructs a new projection operator.
     * 
     * @param operator the input operator.
     * @param slots the slots to be projected.
     * @throws EngineException thrown whenever the projection operator
     * cannot be properly initialized.
     */
    public Project(Operator operator, int [] slots) throws EngineException {
        super(operator);
        this.slots = slots;
        returnList = new ArrayList<Tuple>();
    } // Project()

    /**
     * Processes an incoming tuple.
     * 
     * @param tuple the tuple to be processed.
     * @param inOp the index of the input operator the tuple to be
     * processed belongs to.
     * @throws EngineException on problems with projecting from the
     * input.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp) 
	throws EngineException {
        
        List<Comparable> newValues = new ArrayList<Comparable>();
        for (int i = 0; i < tuple.size(); i++) {
            if (containsSlot(i)) newValues.add(tuple.getValue(i));
        }

        // I have a bad feeling about this...
        returnList.clear();
        returnList.add(
            new Tuple(new IntermediateTupleIdentifier(tupleCounter++),
                      newValues));
        return returnList;
    } // innerProcessTuple()

    
    /**
     * Is the given slot contained in the projection array?
     * 
     * @param slot the slot to be checked.
     * @return <code>true</code> if the slot is contained in the projection
     * array, <code>false</code> otherwise.
     */
    protected boolean containsSlot(int slot) {
        
        boolean found = false;		
        for (int i = 0; i < slots.length && ! found; i++)
            found = (slots[i] == slot);
        return found;
    } // containsSlot()


    /**
     * Sets the output relation of this projection.
     *
     * @return the output relation of this projection.
     * @throws EngineException if the output relation cannot be
     * properly constructed.
     */
    @Override
    protected Relation setOutputRelation() throws EngineException {
        try {
            Operator incoming = getInputOperator();
            Relation inputRelation = incoming.getOutputRelation();
            List<Attribute> attrs = new ArrayList<Attribute>();
            for (int slot = 0;
                 slot < inputRelation.getNumberOfAttributes(); slot++) {
                if (containsSlot(slot)) {
                    //attrs.addElement(attr);
                    attrs.add(inputRelation.getAttribute(slot));
                    //System.out.println(attrs);
                }
            }
            return new Relation(attrs);
        }
        catch (Exception e) {
            throw new EngineException("Could not set the output relation.", e);
        }
    } // setOutputRelation()

    
    /**
     * Textual representation
     */
    @Override
    protected String toStringSingle() {
        StringBuffer sb = new StringBuffer();
        sb.append("project <");
        for (int i = 0; i < slots.length-1; i++)
            sb.append(slots[i] + ", ");
        if (slots.length >= 1) {
			sb.append(slots[slots.length-1]);
        }
        sb.append(">");
        return sb.toString();
    } // toStringSingle()
    
} // Project
