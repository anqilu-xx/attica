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

/**
 * EngineException: Thrown whenever there is an engine error.
 *
 * @author sviglas
 */
public class EngineException extends Exception {
	
    /**
     * Constructs a new exception.
     * 
     * @param message this exception's message.
     */
    public EngineException(String message) {
        super(message);
    } // EngineException()

    /**
     * Constructs a new exception given the error message and
     * originating throwable.
     *
     * @param msg the error message.
     * @param e the originating throwable.
     */
    public EngineException(String msg, Throwable e) {
        super(msg, e);
    } // EngineException()

} // EngineException
