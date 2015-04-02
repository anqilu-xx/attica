/*
 * Created on Dec 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.server;

import java.util.List;

/**
 * TupleInsertion: A new tuple insertion.
 *
 * @author sviglas
 */
public class TupleInsertion extends Statement {
    
    /** The name of the table where the tuple is to be inserted. */
    private String table;
	
    /** The list of values. */
    private List<Comparable> values;

    
    /**
     * Constructs a new tuple insertion command.
     * 
     * @param table the table where the tuples should be inserted.
     * @param values the values of the tuple.
     */
    public TupleInsertion(String table, List<Comparable> values) {
        this.table = table;
        this.values = values;
    } // TupleInsertion()

    
    /**
     * Returns the table where the tuple is to be inserted.
     * 
     * @return the table where the tuple is to be inserted.
     */
    public String getTableName() {
        return table;
    } // getTableName()

    
    /**
     * Retrieves the values of the tuple.
     * 
     * @return the tuple's values.
     */
    public List<Comparable> getValues() {
        return values;
    } // getValues()

} // TupleInsertion
