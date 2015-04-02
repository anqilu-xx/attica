/*
 * Created on Dec 15, 2003 by sviglas
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

/**
 * InitialProjection: An initial projection -- used to project out
 * attributes before the final projection (this is a hack, really, but
 * it will do for now).
 *
 * @author sviglas
 */
public class InitialProjection extends Projection {
	
    /**
     * Constructs a new initial projection algebraic operator.
     * 
     * @param list the list of attributes to be projected.
     */
    public InitialProjection(List<Variable> list) {
        super(list);
    } // InitialProjection()

    
    /**
     * A textual representation of this projection.
     * 
     * @return this projection's textual representation.
     */
    @Override
    public String toString () {
        return "[i-projection (" + getProjectionList().toString() + ")]";
    } // toString()


} // InitialProjection
