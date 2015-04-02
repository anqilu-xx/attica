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
 * TableDescription: A table description command.
 *
 * @author sviglas
 */
public class TableDescription extends Statement {
	
    /** The name of the table to be described */
    private String tablename;

    
    /**
     * Constructs a new table description command.
     * 
     * @param tablename the name of the table to be described.
     */
    public TableDescription(String tablename) {
        this.tablename = tablename;
    } // TableDescription()

    
    /**
     * Retrieves the name of the table to be described.
     * 
     * @return the table to be described.
     */
    public String getTableName() {
        return tablename;
    } // getTableName()

} // TableDescription()
