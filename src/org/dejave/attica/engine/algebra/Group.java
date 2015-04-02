/*
 * Created on Jan 12, 2015 by sviglas
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
 * Group: Representation of a group operation.
 */
public class Group extends AlgebraicOperator {
    /** The group list of this operation. */
    private List<Variable> groupList;
	
    /**
     * Default constructor.
     */
    public Group() {
        this(new ArrayList<Variable>());
    } // Group()

    
    /**
     * Constructs a new group operator.
     * 
     * @param groupList the group list of this operation.
     */
    public Group(List<Variable> groupList) {
        this.groupList = groupList;
    } // Group()


    /**
     * Retrieves the group's group list.
     *
     * @return an iterable over the group list.
     */
    public Iterable<Variable> groups() {
        return groupList;
    }
	
    /**
     * Retrieves all group variables.
     * 
     * @return the group list of this operation.
     */
    public List<Variable> getGroupList() {
        return groupList;
    } // getGroupList()
	
    /**
     * A textual representation of this operation.
     * 
     * @return this operation's textual representation.
     */
    @Override
    public String toString() {
        return "[group (" + groupList.toString() + ")]";
    } // toString()
} // Group
