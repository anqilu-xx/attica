/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Jan 26, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.algebra;

import java.util.List;
import java.util.ArrayList;

/**
 * @author sviglas
 *
 * Sort: Representation of a sort operation.
 */
public class Sort extends AlgebraicOperator {
    /** The sort list of this operation. */
    private List<Variable> sortList;
	
    /**
     * Default constructor.
     */
    public Sort() {
        this(new ArrayList<Variable>());
    } // Sort()

    
    /**
     * Constructs a new sort operator.
     * 
     * @param sortList the sort list of this operation.
     */
    public Sort(List<Variable> sortList) {
        this.sortList = sortList;
    } // Sort()


    /**
     * Retrieves the sort's sort list.
     *
     * @return an iterable over the sort list.
     */
    public Iterable<Variable> sorts() {
        return sortList;
    }
	
    /**
     * Retrieves all sort variables.
     * 
     * @return the sort list of this operation.
     */
    public List<Variable> getSortList() {
        return sortList;
    } // getSortList()
	
    /**
     * A textual representation of this operation.
     * 
     * @return this operation's textual representation.
     */
    @Override
    public String toString() {
        return "[sort (" + sortList.toString() + ")]";
    } // toString()
} // Sort
