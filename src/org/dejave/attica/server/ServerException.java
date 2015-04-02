/*
 * Created on Dec 14, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.server;

/**
 * ServerException: The basic exception thrown for all DB server
 * errors.
 *
 * @author sviglas
 */
public class ServerException extends Exception {
	
    /**
     * Constructs a new server exception.
     * 
     * @param msg this server exception's message.
     */
    public ServerException(String msg) {
        super(msg);
    } // ServerException()

    
    /**
     * Constructs a new server exception given a message and
     * originating throwable.
     *
     * @param msg the error message.
     * @param e the originating throwable.
     */
    public ServerException(String msg, Throwable e) {
        super(msg, e);
    } // ServerException()

} // ServerException
