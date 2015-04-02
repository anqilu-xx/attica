/*
 * Created on Dec 7, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import org.dejave.attica.model.Table;

/**
 * TableIOManager: Convenience class to extend RelationIOManager for
 * tables.
 *
 * @author sviglas
 */
public class TableIOManager extends RelationIOManager {
	
    /**
     * Constructs a new table manager.
     * 
     * @param sm the storage manager for this table manager.
     * @param table the table this manager manages.
     * @param filename the name of the file the table is stored
     * in.
     */
    public TableIOManager(StorageManager sm, Table table, String filename) {
        super(sm, table, filename);
    } // TableIOManager()
    
} // TableIOManager
