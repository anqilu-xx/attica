/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
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
 * Projection: The algebraic representation of a projection.
 *
 * @author sviglas
 */
public class Projection extends AlgebraicOperator {
	
    /** The projection list of this projection. */
    private List<Variable> projectionList;
	
    /**
     * Default constructor.
     */
    public Projection() {
        this(new ArrayList<Variable>());
    } // Projection()

    
    /**
     * Constructs a new projection.
     * 
     * @param projectionList the projection list of this projection.
     */
    public Projection(List<Variable> projectionList) {
        this.projectionList = projectionList;
    } // Projection()

    
    /**
     * Retrieves this projection's projection list.
     * 
     * @return the projection list of this projection.
     */
    public Iterable<Variable> projections() {
        return projectionList;
    } // getProjections()


    /**
     * Retrieves all projection variables.
     *
     * @return all projection variables.
     */
    public List<Variable> getProjectionList() {
        return projectionList;
    }

    
    /**
     * A textual representation of this projection.
     * 
     * @return this projection's textual representation
     */
    @Override
    public String toString() {
        return "[projection (" + projectionList.toString() + ")]";
    } // toString()

} // Projection
