/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.algebra;

import org.dejave.util.Pair;

/**
 * Variable: Encapsulates a qualification/projection variable.
 *
 * @author sviglas
 */
public class Variable extends Pair<String, String> {
	
    /**
     * Constructs a new variable.
     * 
     * @param table the table of this variable.
     * @param attribute the attribute of this variable.
     */
    public Variable(String table, String attribute) {
        super(table, attribute);
    } // Variable()

    
    /**
     * Returns the table of this variable.
     * 
     * @return this variable's table.
     */
    public String getTable() {
        return first;
    } // getTable()

    
    /**
     * Returns the attribute of this variable.
     * 
     * @return this variable's attribute.
     */
    public String getAttribute() {
        return second;
    } // getAttribute()
	
} // Variable
