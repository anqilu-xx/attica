/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.ArrayList;

import org.dejave.attica.model.Attribute;
import org.dejave.attica.model.Relation;
import org.dejave.attica.model.Table;

/**
 * StorageManager: The storage manager of attica.  Provides low-level
 * page I/O to the rest of the system.
 *
 * @author sviglas
 */
public class StorageManager {

    /** The storage manager's catalog. */
    private Catalog catalog;
	
    /** The storage manager's buffer manager. */
    private BufferManager buffer;

    /**
     * Initializes a new storage manager, given a catalog and a buffer
     * pool.
     * 
     * @param catalog this storage mananger's catalog.
     * @param buffer the buffer pool.
     */
    public StorageManager(Catalog catalog, BufferManager buffer) {
        this.catalog = catalog;
        this.buffer = buffer;
    } // StorageManager()


    /**
     * Registers a page with the buffer pool.
     *
     * @param page the page to be registered.
     * @throws StorageManagerException if the page cannot be registered.
     */
    public void registerPage(Page page) throws StorageManagerException {
        //buffer.putPage(page);
        writePage(page);
    } // registerPage()

    
    /**
     * Writes a page to disk.
     * 
     * @param page the page to be written
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public synchronized void writePage(Page page) 
	throws StorageManagerException {
        
        try {
            // put the page in the buffer pool and check whether a
            // page has been evicted -- if it has, flush it            
            Page evictedPage = buffer.putPage(page, false);
            if (evictedPage != null) {
                String fn = evictedPage.getPageIdentifier().getFileName();
                DatabaseFile dbf = new DatabaseFile(fn,
                                                    DatabaseFile.READ_WRITE);
                PageIOManager.writePage(dbf, evictedPage);
                dbf.close();
            }
        }
        catch (Exception e) {
            throw new StorageManagerException("Error writing page to disk.", e);
        }
    } // writePage()

    
    /**
     * Reads a page from the database given the page's identifier.
     * 
     * @param pageid the identifier of the page to be read.
     * @return the page read.
     * @throws StorageManagerException whenever something goes wrong with
     * reading the page.
     */
    public synchronized Page readPage(Relation relation, 
                                      PageIdentifier pageid) 
	throws StorageManagerException {
        
        try {
            // if the buffer has the page read it from there
            if (buffer.containsPage(pageid)) {
                return buffer.getPage(pageid);
            }
            // otherwise read it from file and put it in the buffer pool
            else {
                String fn = pageid.getFileName();
                DatabaseFile dbf =
                    new DatabaseFile(fn, DatabaseFile.READ_WRITE);
                Page page = PageIOManager.readPage(relation, pageid, dbf);
                dbf.close();
                Page evictedPage = buffer.putPage(page, true);
                if (evictedPage != null) {
                    fn = evictedPage.getPageIdentifier().getFileName();
                    dbf = new DatabaseFile(fn, DatabaseFile.READ_WRITE);
                    PageIOManager.writePage(dbf, evictedPage);
                    dbf.close();
                }
                return page;
            }
        }
        catch (Exception e) {
            throw new StorageManagerException("Error reading page from "
                                              + "disk.", e);
        }
    } // readPage()

    
    /**
     * Given a relation, it creates a file name for it.
     * 
     * @param relation the relation for which a file name is to be created.
     * @return the filename associated with the relation.
     */
    public static String createFileName(Relation relation) {
        return new String(relation + "_" + relation.hashCode());
    } // createFileName()


    /**
     * Creates a new table in the database.
     * 
     * @param table the new table.
     * @throws StorageManagerException thrown whenever the table
     * cannot be created.
     */
    public void createTable(Table table) throws StorageManagerException {
        
        try {
            CatalogEntry entry = new CatalogEntry(table);
            catalog.createNewEntry(entry);
            createFile(catalog.getTableFileName(table.getName()));
        }
        catch (IllegalArgumentException iae) {
            throw new StorageManagerException("A table by the name "
                                              + table.getName()
                                              + " already exists", iae);
        }
        catch (NoSuchElementException nsee) {
            throw new StorageManagerException("Could not retrieve the table " +
                                              "that was just created", nsee);
        }
    } // createTable()
    
    
    /**
     * Deletes the specified table from the database.
     * 
     * @param tablename the name of the table to be deleted.
     * @throws NoSuchElementException thrown if the table does not
     * exist.
     * @throws StorageManagerException thrown if the file cannot be
     * deleted.
     */
    public void deleteTable(String tablename) 
	throws NoSuchElementException, StorageManagerException {
        deleteFile(catalog.getTableFileName(tablename));
        catalog.deleteTable(tablename);
    } // deleteTable()


    /**
     * Create a new file by the given file name.
     * 
     * @param filename the name of the new file.
     * @throws StorageManagerException thrown whenever the new file
     * cannot be deleted.
     */
    public void createFile(String filename) throws StorageManagerException {
        
        try {
            
            File file = new File(filename);
            if (! file.createNewFile()) {
                file.delete();
                file.createNewFile();
            } 
        }
        catch (Exception e) {
            throw new StorageManagerException("Could not create file "
                                              + filename + ".", e);
        }
    } // createFile()

    
    /**
     * Delete the specified file.
     * 
     * @param filename the name of the file to be deleted.
     * @throws StorageManagerException thrown whenever the file cannot be
     * deleted.
     */
    public void deleteFile(String filename) throws StorageManagerException {
        
        try {
            // invalidate the file's pages from the buffer manager
            buffer.invalidate(filename);
            File file = new File(filename);
            file.delete();
        }
        catch (Exception e) {
            throw new StorageManagerException("Could not delete file "
                                              + filename + ".", e);
        }
    } // deleteFile()    


    /**
     * Casts the list of comparables to the correct types before
     * inserting the tuple.
     *
     * @param tablename the target table.
     * @param tuple the new tuple.
     * @throws NoSuchElementException thrown whenever the specified table
     * does not exist.
     * @throws StorageManagerException thrown whenever ther insertion is
     * not possible.
     */
    public void castAndInsertTuple(String tablename, Tuple tuple)
        throws NoSuchElementException, StorageManagerException {

        Table table = catalog.getTable(tablename);
        String file = catalog.getTableFileName(tablename);
        TableIOManager man = new TableIOManager(this, table, file);
        man.castAttributes(tuple);
        man.insertTuple(tuple);
    } // castAndInsertTuple()
    
    
    /**
     * Inserts a new tuple into the given table.
     * 
     * @param tablename the name of the table where the tuple is to 
     * be inserted.
     * @param tuple the new tuple.
     * @throws NoSuchElementException thrown whenever the specified table
     * does not exist.
     * @throws StorageManagerException thrown whenever ther insertion is
     * not possible.
     */
    public void insertTuple(String tablename, Tuple tuple)
	throws NoSuchElementException, StorageManagerException {
        
        Table table = catalog.getTable(tablename);
        String file = catalog.getTableFileName(tablename);
        TableIOManager man = new TableIOManager(this, table, file);
        man.insertTuple(tuple);
    } // insertTuple()


    /**
     * Casts the list of comparables to the correct types before
     * inserting the tuple.
     *
     * @param tablename the target table.
     * @param values the values to be inserted.
     * @throws NoSuchElementException thrown whenever the specified table
     * does not exist.
     * @throws StorageManagerException thrown whenever ther insertion is
     * not possible.
     */
    public void castAndInsertTuple(String tablename,
                                   List<Comparable> values)
        throws NoSuchElementException, StorageManagerException {

        Table table = catalog.getTable(tablename);
        String file = catalog.getTableFileName(tablename);
        TableIOManager man = new TableIOManager(this, table, file);
        man.castAttributes(values);
        man.insertTuple(values);
    } // castAndInsertTuple()
    
    
    /**
     * Inserts a new tuple into the given table.
     * 
     * @param tablename the name of the table where the tuple is to 
     * be inserted.
     * @param values the values of the tuple.
     * @throws NoSuchElementException thrown whenever the specified table
     * does not exist.
     * @throws StorageManagerException thrown whenever ther insertion is
     * not possible.
     */
    public void insertTuple(String tablename,
                            List<Comparable> values) 
	throws NoSuchElementException, StorageManagerException {
        
        Table table = catalog.getTable(tablename);
        String file = catalog.getTableFileName(tablename);
        TableIOManager man = new TableIOManager(this, table, file);
        man.insertTuple(values);
    } // insertTuple()

    
    public void shutdown() throws StorageManagerException {

        try {
            for (Page page : buffer.pages()) {
                // this loop is probably the worst thing I've ever
                // written. there should be a per-file dump through a
                // registry of open files and yada-yada-yada... but
                // it's too late in the game for such a rewrite
                String fn = page.getPageIdentifier().getFileName();
                DatabaseFile dbf = new DatabaseFile(fn,
                                                    DatabaseFile.READ_WRITE);
                PageIOManager.writePage(dbf, page);
                dbf.close();
            }
        }
        catch (Exception e) {
            throw new StorageManagerException("Could not properly shut down "
                                              + "the storage manager: "
                                              + e.getMessage(), e);
        }
    } // shutdown()

    /**
     * Returns the number of buffer pool pages.
     *
     * @return the number of buffer pool pages.
     */
    public int getNumberOfBufferPoolPages() {
        return buffer.getNumberOfPages();
    } // getNumberOfBufferPoolPages()
    
    /**
     * Debug main().
     * 
     * @param args
     */
    public static void main (String args[]) {
        try {
            String filename = args[0];
            
            List<Attribute> attrs = new ArrayList<Attribute>();
            attrs.add(new Attribute("character", Character.class));
            attrs.add(new Attribute("byte", Byte.class));
            attrs.add(new Attribute("short", Short.class));
            attrs.add(new Attribute("integer", Integer.class));
            attrs.add(new Attribute("long", Long.class));
            attrs.add(new Attribute("float", Float.class));
            attrs.add(new Attribute("double", Double.class));
            attrs.add(new Attribute("string", String.class));
            Relation rel = new Relation(attrs);
            
            List<Comparable> v = new ArrayList<Comparable>();
            v.add(new Character('a'));
            v.add(new Byte((byte) 26));
            v.add(new Short((short) 312));
            v.add(new Integer(2048));
            v.add(new Long(34567));
            v.add(new Float(12.3));
            v.add(new Double(25.6));
            v.add(new String("bla bla"));
            
            PageIdentifier pid1 = new PageIdentifier(filename, 0);
            Page p1 = new Page(rel, pid1);
            for (int i = 0; i < 20; i++) {
                Tuple t = new Tuple(new TupleIdentifier(filename, i), v);
                p1.addTuple(t);
            }
            System.out.println(p1);
            PageIdentifier pid2 = new PageIdentifier(filename, 1);
            Page p2 = new Page(rel, pid2);
            for (int i = 0; i < 30; i++) {
                Tuple t = new Tuple(new TupleIdentifier(filename, i), v);
                p2.addTuple(t);
            }
            System.out.println(p2);
            
            System.out.println("Writing pages");
            DatabaseFile dfb = new DatabaseFile(filename, 
                                                DatabaseFile.READ_WRITE);
            
            StorageManager sm = new StorageManager(null, 
                                                   new BufferManager(100));
            sm.writePage(p1);
            sm.writePage(p2);
            
            System.out.println("Reading pages");
            p1 = sm.readPage(rel, p1.getPageIdentifier());
            p2 = sm.readPage(rel, p2.getPageIdentifier());
            
            System.out.println(p1);
            System.out.println(p2);
        }
        catch (Exception e) {
            System.err.println("Exception e: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()
} // StorageManager
