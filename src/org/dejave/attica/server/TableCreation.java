/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.server;

import org.dejave.attica.model.Table;

/**
 * TableCreation: Encapsulates a table creation command.
 *
 * @author sviglas
 */
public class TableCreation extends Statement {
	
    /** The description of the table to be created. */
    private Table table;

    
    /**
     * Constructs a new table creation command.
     * 
     * @param table the declaration of the table to be constructed.
     */
    public TableCreation(Table table) {
        this.table = table;
    } // TableCreation()

    
    /**
     * Retrieves the table to be created.
     * 
     * @return the table to be created.
     */
    public Table getTable() { 
        return table;
    } // getTable()

} // TableCreation
