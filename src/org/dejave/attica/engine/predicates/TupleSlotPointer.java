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

/**
 * TupleSlotPointer: A pointer to a slot of a tuple.
 *
 * @author sviglas
 */
public class TupleSlotPointer {

    /** The slot type. */
    private Class<? extends Comparable> type;
	
    /** The slot number. */
    private int slot;

    
    /**
     * Constructs a new tuple slot pointer given the slot.
     *
     * @param type the type of the slot.
     * @param slot the number of the slot.
     */
    public TupleSlotPointer(Class<? extends Comparable> type, int slot) {
        this.type = type;
        this.slot = slot;
    } // TupleSlotPointer()

    
    /**
     * Returns the slot this pointer points to.
     * 
     * @return this pointer's slot.
     */
    public int getSlot() {
        return slot;
    } // getSlot()


    /**
     * Returns the type of this slot.
     *
     * @return this slot's type.
     */
    public Class<? extends Comparable> getType() {
        return type;
    } // getType()
    
	
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return "@[" + getSlot() + "," + getType() + "]";
    } // toString()
    
} // TupleSlotPointer
