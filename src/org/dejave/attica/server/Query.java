/*
 * Created on Dec 26, 2003 by sviglas
 *
 * Modified on Dec 27, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.server;

import java.util.List;

import org.dejave.attica.engine.algebra.AlgebraicOperator;

/**
 * Query: Encapsulates a user query.
 *
 * @author sviglas
 */
public class Query extends Statement {
	
    /** The algebra for this query. */
    private List<AlgebraicOperator> algebra;
    
    /**
     * Constructs a new query.
     * 
     * @param algebra the algebra for the query.
     */
    public Query(List<AlgebraicOperator> algebra) {
        this.algebra = algebra;
    } // Query()

    
    /**
     * Retrieve the algebra for this query.
     * 
     * @return this query's algebra.
     */
    public List<AlgebraicOperator> getAlgebra() {
        return algebra;
    } // getAlgebra()
} // Statement
