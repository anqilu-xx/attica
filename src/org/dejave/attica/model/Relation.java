/*
 * Created on Oct 6, 2003 by sviglas
 *
 * Modified on Dec 20, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Relation: The attica encapsulation of a relation schema.
 *
 * @author sviglas
 */
public class Relation implements java.io.Serializable, Iterable<Attribute> {
    
    /** The attributes of this relation. */
    private List<Attribute> attributes;

    
    /** Default constructor for a relation. */
    public Relation() {
        attributes = new ArrayList<Attribute>();
    } // Relation()

    
    /**
     * Constructs a relation given a <code>List</code> of attributes.
     * 
     * @param attributes the attributes of the relation.
     */
    public Relation(List<Attribute> attributes) {
        this.attributes = new ArrayList<Attribute>(attributes);
    } // Relation()

    
    /**
     * Copy constructor for a relation.
     * 
     * @param relation the relation to be copied.
     */
    public Relation(Relation relation) {
        attributes = new ArrayList<Attribute>();
        for (Attribute attr : relation) 
            addAttribute(attr);
    } // Relation()


    /**
     * Adds an attribute to this relation.
     *
     * @param attribute the attribute to be added.
     */
    protected void addAttribute(Attribute attribute) {
        attributes.add(attribute);
    } // addAttribute()

    
    /**
     * Returns the number of attributes of this relation.
     * 
     * @return this <code>Relation</code>'s number of attributes
     */
    public int getNumberOfAttributes() {
        return attributes.size();
    } // getNumberOfAttributes()

    
    /**
     * Returns an <code>Iterator</code> over the attributes of this
     * relation.
     * 
     * @return this <code>Relation</code>'s attribute
     * <code>Iterator</code>.
     */
    public Iterator<Attribute> iterator() {
        return attributes.iterator();
    } // getAttributesIterator()
    

    /**
     * Returns the specified attribute of this relation.
     * 
     * @param i the index of the attribute to be returned.
     * @return the specified attribute.
     */
    public Attribute getAttribute(int i) {
        return attributes.get(i);
        //return ModelFactory.castAttribute(attributes[i]);
    } // getAttribute()

    
    /**
     * Given an attribute name, it returns the index of the attribute
     * in the relation schema. It returns <code>-1</code> if the
     * attribute does not appear in the schema.
     * 
     * @param attr the attribute name
     * @return the index of the attribute in the relation schema,
     * <code>-1</code> if the attribute does not appear in the schema.
     */
    public int getAttributeIndex(String attr) {        

        int i = 0;
        for (Attribute attribute : attributes)
            if (attribute.getName().equals(attr)) 
                return i;
        return -1;
    } // getAttributeIndex()


    /**
     * Textual representation.
     */
    @Override
    public String toString () {
        
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        for (int i = 0; i < getNumberOfAttributes()-1; i++)
            sb.append(getAttribute(i) + ", ");
        sb.append(getAttribute(getNumberOfAttributes()-1) + "}");
        return sb.toString();
    } // toString()
    
} // Relation()
