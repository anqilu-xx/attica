/*
 * Created on Dec 7, 2003 by sviglas
 *
 * Modified on Dec 17, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * FileUtil: Basic file utilities.
 *
 * @author sviglas
 */
public class FileUtil {

    /** Counter for files. */
    private static long next = 0;
    
    /**
     * Returns the size (in bytes) of the file corresponding to the
     * given filename.
     * 
     * @param filename the name of the file to be checked.
     * @return the size (in bytes) of the file corresponding to the
     * given filename.
     * @throws IOException generic I/O exception.
     * @throws FileNotFoundException thrown whenever the file does not
     * exist.
     */
    public static long getFileSize(String filename) 
	throws IOException, FileNotFoundException {
        
        DatabaseFile dbf = new DatabaseFile(filename, DatabaseFile.READ);
        long size = dbf.length();
        dbf.close();
        return size;
    } // getFileSize()

    
    /**
     * Return the number of attica pages in the file corresponding to
     * the given filename.
     * 
     * @param filename the name of the file to be checked.
     * @return the number of attica pages in the file corresponding to the
     * given filename.
     * @throws IOException generic I/O exception.
     * @throws FileNotFoundException thrown whenever the file does not
     * exist.
     */
    public static int getNumberOfPages(String filename) 
	throws IOException, FileNotFoundException {
        
        DatabaseFile dbf = new DatabaseFile(filename, DatabaseFile.READ);
        int pages = (int) (dbf.length() / Sizes.PAGE_SIZE + .5);
        dbf.close();
        return pages;
    } // getNumberOfPages()


    public static void setNumberOfPages(String filename, int np)
        throws IOException, FileNotFoundException {

        DatabaseFile dbf = new DatabaseFile(filename, DatabaseFile.READ_WRITE);
        int pages = (int) (dbf.length() / Sizes.PAGE_SIZE + .5);
        if (pages < np) dbf.setLength(np*Sizes.PAGE_SIZE);
        dbf.close();
    }
    
    /**
     * Create a new temporary file name.
     * 
     * @return a new temp file name.
     */
    public static String createTempFileName() {
        
        return org.dejave.attica.server.Database.TEMP_DIR
            +  System.getProperty("file.separator")
            + "attica-" + (next++) + ".tmp";
    } // createFileName()

} // FileUtil
