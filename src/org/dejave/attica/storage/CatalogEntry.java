/*
 * Created on Nov 25, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.io.Serializable;

import org.dejave.attica.model.Table;

/**
 * CatalogEntry: Stores all information pertinent to a table in the
 * catalog.
 *
 * @author sviglas
 */
public class CatalogEntry implements Serializable {
	
    /** The table for this catalog entry. */
    private Table table;
	
    /** The filename of the table. */
    private String fileName;

    
    /**
     * Creates a new catalog entry given the table.
     * 
     * @param table the table name for this catalog entry.
     */
    public CatalogEntry(Table table) {
        
        this.table = table;
        createFileName();
    } // CatalogEntry()

    
    /**
     * The name of the table this entry corresponds to.
     * 
     * @return this catalog entry's table name.
     */
    public String getTableName() {
        return table.getName();
    } // getTableName()

    
    /**
     * Returns this catalog entry's table.
     * 
     * @return the table this catalog entry corresponds to.
     */
    public Table getTable() {
        return table;
    } // getTable()

    
    /**
     * Returns the filename of this catalog entry's table's filename.
     * 
     * @return This catalog entry's table's filename
     */
    public String getFileName() {
        return fileName;
    } // getFileName()

    
    /**
     * Creates a new filename for the entry's table name.
     */
    protected void createFileName() {
        
        String tableName = table.getName();
        fileName = new String(org.dejave.attica.server.Database.ATTICA_DIR
                              + System.getProperty("file.separator")
                              + tableName + "_" + tableName.hashCode());
    } // createFileName()

    
    /**
     * String representation.
     * 
     * @return this entry's string representation.
     */
    @Override
    public String toString() {
        return "Table: " + getTableName() + ", filename: "
            + fileName + ", def: " + table;
    } // toString()

} // CatalogEntry
