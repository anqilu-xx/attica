/*
 * Created on Dec 5, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * DatabaseFile: A paged database file.
 *
 * @author sviglas
 */
public class DatabaseFile extends RandomAccessFile {
    
    /** Constant signifying read access to a file */
    public static final String READ = "r";
    
    /** Constant signifying read/write access to a file. */
    public static final String READ_WRITE = "rw";
    
    /**
     * Constructs a new DatabaseFile instance.
     * 
     * @param filename the name of the file.
     * @param mode the mode in which the file is to be opened.
     * @throws FileNotFoundException thrown whenever the file is not
     * found.
     */
    public DatabaseFile(String filename, String mode) 
	throws FileNotFoundException {
        super(filename, mode);
    } // DatabaseFile()
    
} // DatabaseFile
