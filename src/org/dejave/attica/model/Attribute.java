/*
 * Created on Oct 5, 2003 by sviglas
 *
 * Modified on Dec 20, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.model;


/**
 * Attribute: The basic abstraction of an attica data model Attribute.
 *
 * @author sviglas
 */
public class Attribute implements java.io.Serializable {
    
    /** The name of this attribute. */
    private String name;

    /** The type of this attribute. */
    private Class<? extends Comparable> type;

    
    /**
     * Create a new attribute given its name and class.
     * 
     * @param name the name of the attribute.
     * @param type the class of the attribute.
     */
    public Attribute(String name, Class<? extends Comparable> type) {
        this.name = name;
        this.type = type;
    } // Attribute()
    
		
    /**
     * Copy constructor for an attribute.
     * 
     * @param attribute the attribute to be copied.
     */
    public Attribute(Attribute attribute) {
        this.name = attribute.getName();
        this.type = attribute.getType();
    } // Attribute()

    
    /**
     * Return the name of this attribute.
     * 
     * @return this attribute's name.
     */
    public String getName() {
        return name;
    } // getName()

    
    /**
     * Return the class of this attribute.
     * 
     * @return this attribute's type.
     */
    public Class<? extends Comparable> getType() {
        return type;
    } // getType()

    public boolean equals(Object o) {
        if (this == o) return true;
        if (! (o instanceof Attribute)) return false;
        Attribute a = (Attribute) o;
        return getName().equals(a.getName()) && getType().equals(a.getType());
    }

    public int hashCode() {
        int hash = 17;
        hash = hash*31 + getName().hashCode();
        return hash*31 + getType().hashCode();
    }

    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return getName() + ":" + getType();
    } // toString()
    
} // Attribute
