/*
 * Created on Dec 5, 2003 by sviglas
 *
 * Modofied on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

import org.dejave.attica.model.Relation;
import org.dejave.util.Convert;
import org.dejave.util.Pair;

/**
 * PageIOManager: Implements page I/O over attica files.
 *
 * @author sviglas
 */
public class PageIOManager {
	
    /**
     * Constructs a new page I/O manager.
     */
    public PageIOManager() {} 
	
    /**
     * Writes a page to an output file.
     * 
     * @param raf the output file.
     * @param page the page to be written.
     * @throws StorageManagerException thrown whenever there is an
     * output error.
     */
    public static void writePage(RandomAccessFile raf, Page page) 
	throws StorageManagerException {
        
        try {
            // seek to the correct place in the file
            long seek = page.getPageIdentifier().getNumber() * Sizes.PAGE_SIZE;
            raf.seek(seek);
            byte [] bytes = new byte[Sizes.PAGE_SIZE];
            dumpNumber(page, bytes);
            dumpTuples(page, bytes);
            raf.write(bytes);
        }
        catch (IOException ioe) {
            throw new StorageManagerException("Exception while writing page "
                                              + page.getPageIdentifier()
                                              + " to disk.", ioe);
        }
    } // writePage()

    
    /**
     * Reads a page from disk.
     * 
     * @param relation the relation the requested page conforms to.
     * @param pid the page identifier of the page.
     * @param raf the output file.
     * @return the page read.
     * @throws StorageManagerException whenever the page cannot be
     * properly read.
     */
    public static Page readPage(Relation relation, PageIdentifier pid, 
                                RandomAccessFile raf) 
	throws StorageManagerException {
        
        try {
            // seek to the appropriate place
            long seek = pid.getNumber() * Sizes.PAGE_SIZE;
            raf.seek(seek);
            byte [] bytes = new byte[Sizes.PAGE_SIZE];
            int bytesRead = raf.read(bytes);
            if (bytesRead == -1) {
                // we've reached the end of file, so we need to
                // allocate a page
                raf.setLength(seek + Sizes.PAGE_SIZE);
                return new Page(relation, pid);
            }
            if (bytesRead != Sizes.PAGE_SIZE) {
                throw new StorageManagerException("Page " + pid.toString()
                                                  + "was not fully read.");
            }
            return fetchTuples(relation, pid, bytes);
        }
        catch (IOException ioe) {
            throw new StorageManagerException("Exception while reading page "
                                              + pid.toString()
                                              + " from disk.", ioe);
        }
    } // readPage()


    /**
     * Dumps the number of tuples of the page.
     *
     * @param page the page to be written.
     * @param bytes the output byte array.     
     */     
    protected static void dumpNumber(Page page, byte [] bytes) {
        
        byte [] b = Convert.toByte(page.getNumberOfTuples());
        System.arraycopy(b, 0, bytes, 0, b.length);
    } // dumpNumber()

    
    /**
     * Dump the page tuples to disk.
     * 
     * @param page the page to be written to disk.
     * @param bytes the output byte array for the tuples.
     * @throws StorageManagerException if the 
     */
    protected static void dumpTuples(Page page, byte [] bytes)
        throws StorageManagerException {
        
        // start up a new tuple IO manager
        TupleIOManager manager =
            new TupleIOManager (page.getRelation(),
                                page.getPageIdentifier().getFileName());
        // one integer was used for the number of tuples
        int offset = Convert.INT_SIZE;
        // iterate over all tuples, place them in the array
        for (Tuple tuple : page)
            offset = manager.writeTuple(tuple, bytes, offset);
        pad(bytes, offset);
    } // dumpTuples()
    
    /**
     * Reads in tuples from disk and puts them in a new page.
     * 
     * @param relation the relation the page conforms to.
     * @param pid the page identifier of the new page.
     * @param bytes the byte array where the tuples are in.
     * @return the page read from disk.
     */
    protected static Page fetchTuples(Relation relation, PageIdentifier pid, 
                                      byte [] bytes) 
	throws StorageManagerException {
        
        // start up a new tuple IO manager
        TupleIOManager manager = new TupleIOManager(relation,
                                                    pid.getFileName());
        // start reading tuples
        int numberOfTuples = fetchNumber(bytes);
        Page page = new Page(relation, pid);
        //page.setNumberOfTuples(numberOfTuples);
        // one integer for the number of tuples
        int offset = Convert.INT_SIZE;
        for (int i = 0; i < numberOfTuples; i++) {
            Pair pair = manager.readTuple(bytes, offset);
            Tuple tuple = (Tuple) pair.first;
            offset = ((Integer) pair.second).intValue();
            page.addTuple(tuple);
        }
		
        return page;
    } // fetchTuples()

    /**
     * Fetches the number of tuples from a byte array.
     *
     * @param bytes the byte array.
     * @return the number of tuples.
     */
    public static int fetchNumber(byte [] bytes) {
        
        byte [] b = new byte[Convert.INT_SIZE];
        System.arraycopy(bytes, 0, b, 0, b.length);
        return Convert.toInt(b);
    } // fetchNumber()

    
    /**
     * Pad a byte array with zeros to reach the disk page size.  (Not
     * sure this will ever be used, but whatever...)
     * 
     * @param bytes the input byte array to be padded.
     * @param start the starting offset in the byte array.
     */
    protected static void pad(byte [] bytes, int start) {
        for (int i = start; i < bytes.length; i++) bytes[i] = (byte) 0;        
    } // pad()

} // PageIOManager
