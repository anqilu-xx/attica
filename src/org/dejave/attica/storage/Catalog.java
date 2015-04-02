/*
 * Created on Oct 10, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Hashtable;
import java.util.NoSuchElementException;

import org.dejave.attica.model.Table;
import org.dejave.attica.model.Attribute;

/**
 * Catalog: The catalog abstraction of the attica storage manager.
 *
 * (By the way, I know this implementation is the lamest, most
 * brain-damaged one I could come up with.  I'm just too lazy to
 * bootstrap the database catalog properly.)
 *
 * @author sviglas
 */
public class Catalog {
	
    /** The name of the catalog file. */
    private String catalogFile;
	
    /** The entries of the catalog, mapping table names to entries. */
    private Map<String, CatalogEntry> entries;
    
    /** The number of tables currently stored in the catalog. */
    private int numberOfTables;

    
    /**
     * Construct a new catalog given the name of the catalog file.
     * 
     * @param catalogFile the name of the catalog file.
     */
    public Catalog(String catalogFile) {
        
        this.catalogFile = catalogFile;
        entries = new Hashtable<String, CatalogEntry>();
    } // Catalog()
    
    
    /**
     * Creates an entry into the catalog for a new table.
     * 
     * @param entry the catalog entry for the new catalog.
     * @throws IllegalArgumentException thrown whenever the user
     * tries to create a table with the same name as an existing one.
     */
    public void createNewEntry(CatalogEntry entry) 
	throws IllegalArgumentException {
        
        if (! tableExists(entry.getTableName())) {
            entries.put(entry.getTableName(), entry);
            numberOfTables++;
        }
        else {
            throw new IllegalArgumentException("Table: "
                                               + entry.getTableName()
                                               + " already exists.");
        }
    } // createNewEntry()

    
    /**
     * Checks to see whether the given table exists in the catalog.
     * 
     * @param tableName the name of the table to be checked.
     * @return <pre>true</pre> if the table exists, <pre>false</pre>
     * otherwise.
     */
    protected boolean tableExists(String tableName) {
        return (entries.get(tableName) != null);
    } // tableExists()

    
    /**
     * Reads in the catalog from the file specified by the filename
     * passed as an argument to the constructor.
     * 
     * @throws StorageManagerException thrown whenever there is
     * something wrong with reading the catalog file
     */
    @SuppressWarnings("unchecked")
    public void readCatalog() throws StorageManagerException {
        
        
        try {            
            FileInputStream fstream = new FileInputStream(catalogFile);
            ObjectInputStream istream = new ObjectInputStream(fstream);
            
            // read in the number of tables
            numberOfTables = istream.readInt();
            // read in the entries of the catalog
            
            // OK, mini-rant time: generics are brain-damaged in this
            // respect. my guess is that it's using the erased type
            // for the safety check, so there's no way to find the
            // instantiated type. makes sense, but it just shows you
            // that the way generics have been implemented loses all
            // type information after compilation and it's just
            // syntactic sugar for type safety -- which cannot be
            // guaranteed across invocations of the same code. yeah,
            // well, **** me gently with a chainsaw...
            entries = (Map<String, CatalogEntry>) istream.readObject();
            
            istream.close();
        }
        catch (ClassCastException cce) {
            throw new StorageManagerException("Could not cast from catalog "
                                              + "file.", cce);
        }
        catch (ClassNotFoundException cnfe) {
            throw new StorageManagerException("Casting error when reading the "
                                              + "catalog entries from file "
                                              + catalogFile
                                              + ". Could not cast to"
                                              + "entry type.", cnfe);
        }
        catch (FileNotFoundException fnfe) {
            throw new StorageManagerException("*CAUTION* catalog file "
                                              + catalogFile + " not found!",
                                              fnfe);
        }
        catch (IOException ioe) {
            throw new StorageManagerException("I/O Exception while opening "
                                              + "the catalog from file "
                                              + catalogFile, ioe);
        }
    } // readCatalog()

    
    /**
     * Writes the catalog into the specified file specified by the
     * filename passed as an argument to the constructor.
     * 
     * @throws StorageManagerException thrown whenever there is
     * something wrong with writing out the catalog file.
     */
    public void writeCatalog() throws StorageManagerException {
        
        try {
            FileOutputStream fstream = new FileOutputStream(catalogFile);
            ObjectOutputStream ostream = new ObjectOutputStream(fstream);
            
            // write out the number of tables
            ostream.writeInt(numberOfTables);
            // write out the entries of the catalog
            ostream.writeObject(entries);
            
            ostream.flush();
            ostream.close(); 
        }
        catch (IOException ioe) {
            throw new StorageManagerException("I/O Exception while storing "
                                              + "the catalog file in "
                                              + catalogFile, ioe);
        }
    } // writeCatalog()

    
    /**
     * Returns the name of the file associated with a table.
     * 
     * @param tableName the table name.
     * @return the name of the file associated with a table.
     * @throws NoSuchElementException if a table with the given file
     * name does not exist in the catalog.
     */
    public String getTableFileName(String tableName) 
	throws NoSuchElementException {
        
        CatalogEntry entry = entries.get(tableName);
        if (entry == null) 
            throw new NoSuchElementException("Table " + tableName + " is not "
                                             + "in the DB catalog.");
        else
            return entry.getFileName();
    } // getTableFileName()

    
    /**
     * Returns the table associated with a table name.
     * 
     * @param tableName the table name
     * @return the table associated with the table name
     * @throws NoSuchElementException whenever the given table does
     * not exist.
     */
    public Table getTable(String tableName) 
	throws NoSuchElementException {
        
        CatalogEntry entry = (CatalogEntry) entries.get(tableName);
        if (entry == null) 
            throw new NoSuchElementException("Table " + tableName + " is not "
                                             + "in the DB catalog.");
        else
            return entry.getTable();
    } // getTable()

    
    /**
     * Deletes a table from the catalog.
     * 
     * @param tablename the name of the table to be deleted.
     * @throws NoSuchElementException thrown if the table is not in
     * the catalog.
     */
    public void deleteTable(String tablename)
        throws NoSuchElementException {
        
        if (! tableExists(tablename))
            throw new NoSuchElementException("Table " + tablename + " is not "
                                             + "in the DB catalog.");
        entries.remove(tablename);
        numberOfTables--;
    } // deleteTable()

    
    /**
     * Returns the string representation of this catalog.
     * 
     * @return the catalog as a string.
     */
    @Override
    public String toString() {
        return "Catalog: " + numberOfTables + " tables.  Entries: \n"
            + "\t" + entries.toString(); 
    } // toString()

    
    /**
     * Debug main()
     */
    public static void main (String args[]) {
        try {
            // create three new table entries
            CatalogEntry ce1 =
                new CatalogEntry(new Table("sailors",
                                           new ArrayList<Attribute>()));
            CatalogEntry ce2 =
                new CatalogEntry(new Table("boats",
                                           new ArrayList<Attribute>()));
            CatalogEntry ce3 =
                new CatalogEntry(new Table("trips",
                                           new ArrayList<Attribute>()));
            
            // create the catalog
            Catalog catalog = new Catalog(args[0]);
            
            // add info to the catalog
            catalog.createNewEntry(ce1);
            catalog.createNewEntry(ce2);
            catalog.createNewEntry(ce3);
            
            // print out the catalog
            System.out.println(catalog);
            
            // write it out
            System.out.println("Flushing catalog.");
            catalog.writeCatalog();
            
            // read it back in
            System.out.println("Reading catalog.");
            catalog.readCatalog();
            
            // print it out again
            System.out.println(catalog);
        }
        catch (Exception e) {
            System.err.println("Exception " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()

} // Catalog
