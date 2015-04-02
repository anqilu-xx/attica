/*
 * Created on Dec 9, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

/**
 * IntermediateTupleIdentifier: A unique identifier for an
 * intermediate tuple.
 *
 * @author sviglas
 */
public class IntermediateTupleIdentifier extends TupleIdentifier {

    /**
     * Constructs a new identifier given its number.
     *
     * @param number the intermediated tuple's identifying number.
     */
    public IntermediateTupleIdentifier(int number) {
        super("", number);
    } // IntermediateTupleIdentifer()
    
} // IntermediateTupleIdentifer
