/*
 * Created on Oct 5, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.util.List;
import java.util.ArrayList;

/**
 * Tuple: The basic encapsulation of an attica <code>Tuple</code>.
 *
 * @author sviglas
 */
public class Tuple {
	
    /** The identifier of this tuple. */
    private TupleIdentifier tupleIdentifier;
    
    /** The values stored in the tuple. */
    private List<Comparable> values;
	
    /**
     * Constructs a new empty <code>Tuple</code>.
     */
    public Tuple() {
        this.tupleIdentifier = null;
        this.values = new ArrayList<Comparable>();
    } // Tuple()

    
    /**
     * Constructs a new tuple given an identifier and a 
     * <code>List</code> of values.
     * 
     * @param tupleIdentifier the identifier of the tuple.
     * @param values the values to be added.
     */
    public Tuple(TupleIdentifier tupleIdentifier, List<Comparable> values) {
        this.tupleIdentifier = tupleIdentifier;
        this.values = values;
        // may have to copy things if there are any problems during
        // updates, but we'll see till then; a reference will do for now
        // this.values = new ArrayList<Comparable>(values);
    } // Tuple()

    
    /**
     * Is this tuple intermediate or not?
     * 
     * @return <code>true</code> if the tuple is intermediate,
     * <code>false</code> otherwise.
     */
    public boolean isIntermediate() {
        // brain-damaged. no, really. brain-damaged.
        return (getTupleIdentifier() instanceof IntermediateTupleIdentifier);
    } // isIntermediate()

    
    /**
     * Returns the identifier of this tuple.
     * 
     * @return this tuple's identifier.
     */
    public TupleIdentifier getTupleIdentifier() {
        return tupleIdentifier;
    } // getTupleIdentifier()


    /**
     * Sets this tuple's identifier.
     *
     * @param td the tuple's identifier.
     */
    public void setTupleIdentifier(TupleIdentifier td) {
        tupleIdentifier = td;
    } // setTupleIdentifier()

    
    /**
     * Sets a value of the tuple. *NOTE* it does not perform any type 
     * or bounds checking.
     * 
     * @param slot the slot of the tuple accessed.
     * @param value the new value of the tuple.
     */
    public void setValue(int slot, Comparable value) {
        values.set(slot, value);
    } // setValue()

    
    /**
     * Sets the values of this tuple.
     * 
     * @param values the vector of new values.
     */
    public void setValues(List<Comparable> values) {        
        this.values.clear();
        this.values = values;
    } // setValues()


    /**
     * Returns the size in slots of this tuple.
     * 
     * @return the number of slots in this tuple.
     */
    public int size() {
        return values.size();
    } // size()

    
    /**
     * Returns the values of this tuple.
     * 
     * @return this tuple's values.
     */
    public List<Comparable> getValues() {
        return values;
    } // getValues()
    
	
    /**
     * Returns the slot of the tuple cast to a primitive character.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * character.
     * @throws ClassCastException if the cast fails.
     */
    public char asChar(int slot) throws ClassCastException {
        Character c  = (Character) values.get(slot);
        return c.charValue();
    } // asChar()

    
    /**
     * Returns the slot of the tuple cast to a primitive byte.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * byte.
     * @throws ClassCastException if the cast fails.
     */
    public byte asByte(int slot) throws ClassCastException {
        Byte b  = (Byte) values.get(slot);
        return b.byteValue();
    } // asByte()

    
    /**
     * Returns the slot of the tuple cast to a primitive short.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * short.
     * @throws ClassCastException if the cast fails.
     */
    public short asShort(int slot) throws ClassCastException {
        Short s  = (Short) values.get(slot);
        return s.shortValue();
    } // asShort()
    
    
    /**
     * Returns the slot of the tuple cast to a primitive integer.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * integer.
     * @throws ClassCastException if the cast fails.
     */
    public int asInt(int slot) throws ClassCastException {
        Integer in = (Integer) values.get(slot);
        return in.intValue();
    } // asInt()

    
    /**
     * Returns the slot of the tuple cast to a primitive long.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * long.
     * @throws ClassCastException if the cast fails.
     */
    public long asLong(int slot) throws ClassCastException {
        Long l = (Long) values.get(slot);
        return l.longValue();
    } // asLong()
	

    /**
     * Returns the slot of the tuple cast to a primitive float.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * float.
     * @throws ClassCastException if the cast fails.
     */
    public float asFloat(int slot) throws ClassCastException {
        Float f = (Float) values.get(slot);
        return f.floatValue();
    } // asFloat() 

    
    /**
     * Returns the slot of the tuple cast to a primitive double.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a primitive
     * double.
     * @throws ClassCastException if the cast fails.
     */
    public double asDouble(int slot) throws ClassCastException {
        Double doub = (Double) values.get(slot);
        return doub.doubleValue();
    } // asDouble() 
	

    /**
     * Returns the slot of the tuple cast to a Java <code>String</code>.
     * 
     * @param slot the slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple cast to a Java
     * <code>String</code>.
     * @throws ClassCastException if the cast fails.
     */
    public String asString(int slot) throws ClassCastException {
        String str = (String) values.get(slot);
        return str;
    } // asString()

    
    /**
     * Returns the specified slot of this tuple as a generic
     * Comparable.
     * 
     * @param slot ths slot of the <code>Tuple</code> accessed.
     * @return the specified slot of the tuple as a generic Comparable.
     */
    public Comparable getValue(int slot) {
        return values.get(slot);
    } // getValue()


    /**
     * Tests this tuple to an object for equality.
     *
     * @param o the object to compare this tuple to.
     * @return <code>true</code> if this tuple is equal to
     * <code>o</code>, <code>false</code> otherwise.
     */
    @Override
    public boolean equals(Object o) {
        
        if (o == this) return true;
        if (! (o instanceof Tuple)) return false;
        Tuple t = (Tuple) o;
        if (size() != t.size()) return false;
        if (! getTupleIdentifier().equals(t.getTupleIdentifier())) return false;
        int i = 0;
        for (Comparable comp : values) {
            if (! comp.equals(t.getValue(i))) return false;
            i++;
        }
        return true;
    }

    /**
     * Returns a hash code for this tuple.
     *
     * @return a hash code for this tuple.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        hash += 31*hash + getTupleIdentifier().hashCode();
        for (Comparable comp : values)
            hash += 31*hash + comp.hashCode();
        return hash;
    }
    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return tupleIdentifier.toString() + " : " + values.toString();
    } // toString()

    public String toStringFormatted() {
        return "(" + tupleIdentifier.getNumber() + ") : " + values.toString();
    }
} // Tuple
