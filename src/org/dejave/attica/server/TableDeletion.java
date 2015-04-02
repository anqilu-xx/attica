/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.server;

/**
 * TableDeletion: Encapsulates a table deletion command.
 *
 * @author sviglas
 */
public class TableDeletion extends Statement {

    /** The name of the table. */
    private String tablename;

    
    /**
     * Constructs a table deletion statement.
     * 
     * @param tablename the name of the table to be deleted.
     */
    public TableDeletion(String tablename) {
        this.tablename = tablename;
    } // TableDeletion()

    
    /**
     * Retrieves the name of the table that is to be deleted.
     * 
     * @return the table to be deleted.
     */
    public String getTableName() {
        return tablename;
    } // getTableName()
	
} // TableDeletion
