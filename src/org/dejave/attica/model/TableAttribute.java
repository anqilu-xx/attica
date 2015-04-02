/*
 * Created on Dec 15, 2003 by sviglas
 *
 * Modified on Dec 22, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.model;

/**
 * TableAttribute: a qualified attribute that appears in a table
 * (wraps a standard attribute).
 *
 * @author sviglas
 */
public class TableAttribute extends Attribute implements java.io.Serializable {
    
    /** The table of this attribute. */
    private String table;

    /**
     * Constructs a new table attribute given table, name and type.
     * 
     * @param table the table name.
     * @param name the name of the attribute.
     * @param type the type of the attribute.
     */
    public TableAttribute(String table, String name,
                          Class<? extends Comparable> type) {
        super(name, type);
        this.table = table;
    } // TableAttribute()
    
    
    /**
     * Copy constructor for table attributes.
     * 
     * @param ta the table attribute to be copied.
     */
    public TableAttribute(TableAttribute ta) {
        this(new String(ta.getTable()), 
             new String(ta.getName()), ta.getType()); 
    } // TableAttribute()
    
    
    /**
     * Retrieves the table of this attribute.
     * 
     * @return this attribute's table.
     */
    public String getTable() {
        return table;
    } // getTable()


    /**
     * Tests this attribute for equality to an object.
     *
     * @param o the object to test for equality.
     * @return <code>true</code> if the two objects are equal,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (! (o instanceof TableAttribute)) return false;
        TableAttribute a = (TableAttribute) o;
        return getName().equals(a.getName())
            && getType().equals(a.getType())
            && getTable().equals(a.getTable());
    }


    /**
     * Returns a hashcode for this attribute.
     *
     * @return a hash code for this attribute.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        hash = hash*31 + getName().hashCode();        
        hash = hash*31 + getType().hashCode();        
        return hash*31 + getTable().hashCode();
    }
    
    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return getTable() + "." + getName() + ": " + getType();
    } // toString()
    
} // TableAttribute
