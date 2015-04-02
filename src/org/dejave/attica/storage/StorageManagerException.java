/*
 * Created on Oct 10, 2003 by sviglas
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
 * StorageManagerException: The base class for all the exceptions of
 * the storage manager.
 *
 * @author sviglas
 */
public class StorageManagerException extends Exception {
	
    /**
     * Creates a new exception to be thrown.
     * 
     * @param message the message of the exeption.
     */
    public StorageManagerException (String message) {
        super(message);
    } // StorageManagerException()


    /**
     * Creates a new exception to be thrown.
     * 
     * @param m the message of the exeption.
     * @param e the originating throwable.
     */
    public StorageManagerException(String m, Throwable e) {
        super(m, e);
    } // StorageManagerException()
} // StorageManagerException
