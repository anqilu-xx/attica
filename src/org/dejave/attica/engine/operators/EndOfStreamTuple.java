/*
 * Created on Dec 8, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import org.dejave.attica.storage.Tuple;

/**
 * EndOfStreamTuple: Signifies the end of a stream.
 *
 * @author sviglas
 */
public class EndOfStreamTuple extends Tuple {
	
    /**
     * Constructs a new end-of-stream tuple.
     */
    public EndOfStreamTuple() {
        super(null, null);
    } // EndOfStreamTuple()
    
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return "** end-of-stream **";
    } // toString()

    @Override
    public String toStringFormatted() {
        return toString();
    } // toStringFormatted()
    
} // EndOfStreamTuple
