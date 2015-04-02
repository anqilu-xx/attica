/*
 * Created on Oct 5, 2003 by sviglas
 *
 * Modified on Dec 22, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.model;

import java.util.List;

/**
 * Table: Encapsulates a <code>Table</code> in the attica data model.
 * A <code>Table</code> is nothing more than a named <code>Relation</code>.
 *
 * @author sviglas
 * @see Relation
 */
public class Table extends Relation implements java.io.Serializable {
	
    /** This table's name. */
    private String name;
    
    /**
     * Constructs a new table given its name.
     * 
     * @param name the name of the <code>Table</code>.
     */
    public Table(String name) {
        super();
        this.name = name;
    } // Table()

    
    /**
     * Constructs a table given a name and <code>List</code> of
     * attributes.
     * 
     * @param name the name of the <code>Table</code>.
     * @param attributes the attributes of the <code>Table</code>.
     */
    public Table(String name, List<Attribute> attributes) {
        super(attributes);
        this.name = name;
    } // Table()

    
    /**
     * Retrieves the name of the table.
     * 
     * @return this <code>Table</code>'s name.
     */
    public String getName() {
        return name;
    } // getName()

} // Table
