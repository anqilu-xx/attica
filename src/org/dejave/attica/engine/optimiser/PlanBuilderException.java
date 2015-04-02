/*
 * Created on Dec 14, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.optimiser;

/**
 * PlanBuilderException: Exception thrown when building a plan.
 *
 * @author sviglas
 */
public class PlanBuilderException extends Exception {
	
    /**
     * Constructs a new exception instance.
     * 
     * @param msg this exception's message.
     */
    public PlanBuilderException(String msg) {
        super(msg);
    } // PlanBuilderException()

    
    /**
     * Constructs a new exception instance given a message and an
     * originating throwable.
     *
     * @param msg the message.
     * @param e the originating throwable.
     */
    public PlanBuilderException(String msg, Throwable e) {
        super(msg, e);
    } // PlanBuilderException()

} // PlanBuilderException
