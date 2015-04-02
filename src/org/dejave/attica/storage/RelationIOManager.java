/*
 * Created on Dec 7, 2003 by sviglas
 *
 * Modified on Dec 22, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.dejave.attica.model.Attribute;
import org.dejave.attica.model.Relation;

import org.dejave.attica.storage.FileUtil;


/**
 * RelationIOManager: The basic class that undertakes relation I/O.
 *
 * @author sviglas
 */
public class RelationIOManager {
	
    /** The relation of this manager. */
    private Relation relation;
	
    /** This manager's storage manager. */
    private StorageManager sm;
	
    /** The filename for the relation. */
    private String filename;
	
    /**
     * Constructs a new relation I/O manager.
     * 
     * @param sm this manager's storage manager.
     * @param relation the relation this manager handles I/O for.
     * @param filename the name of the file this relation is stored
     * in.
     */
    public RelationIOManager(StorageManager sm,
                             Relation relation, 
                             String filename) {
        this.sm = sm;
        this.relation = relation;
        this.filename = filename;
    } // RelationIOManager()
	
    /**
     * Inserts a new tuple into this relation.
     * 
     * @param tuple the tuple to be inserted.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple(Tuple tuple) throws StorageManagerException {
        insertTuple(tuple, true);
    } // insertTuple()

    
    /**
     * Inserts a new tuple specified as a list of comparable values
     * into this relation.
     *
     * @param values the list of comparables to be inserted.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple (List<Comparable> values)
        throws StorageManagerException {
        
        insertTuple(new Tuple(new TupleIdentifier(null, 0), values), true);
    } // insertTuple()

    
    /**
     * Inserts a new tuple into this relation.
     * 
     * @param tuple the tuple to be inserted.
     * @param newID re-assigns the tuple id if set to <pre>true</pre>.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public void insertTuple(Tuple tuple, boolean newID) 
	throws StorageManagerException {

        // to be honest, going over the code I have no idea why we may
        // not be re-assigning the tuple id, but I guess I was
        // thinking of something back then.
        try {
            // read in the last page of the file
            int pageNum = FileUtil.getNumberOfPages(getFileName());
            pageNum = (pageNum == 0) ? 0 : pageNum-1;
            PageIdentifier pid = 
                new PageIdentifier(getFileName(), pageNum);
            Page page = sm.readPage(relation, pid);
            int num = 0;
            if (page.getNumberOfTuples() != 0) {
                Tuple t = page.retrieveTuple(page.getNumberOfTuples()-1);
                num = t.getTupleIdentifier().getNumber()+1;
            }

            if (newID)
                tuple.setTupleIdentifier(new TupleIdentifier(getFileName(),
                                                             num));
            
            //long tn = page.getNumberOfTuples();
            if (! page.hasRoom(tuple)) {
                page = new Page(relation, new PageIdentifier(getFileName(),
                                                             pageNum+1));
                FileUtil.setNumberOfPages(getFileName(), pageNum+2);
            }
            page.addTuple(tuple);
            sm.writePage(page);
        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            throw new StorageManagerException("I/O Error while inserting tuple "
                                              + "to file: " + getFileName()
                                              + " (" + e.getMessage() + ")", e);
        }
    } // insertTuple ()


    /**
     * Wrapper for castAttributes() with a list of comparables as the
     * parameter. See the notes there. (Package visible, because this
     * is embarrassing.)
     *
     * @param crap the crap needed by the other cast attributes
     * method.
     * @throws StorageManagerException thrown whenever the cast is not
     * possible.
     */
    void castAttributes(Tuple crap) throws StorageManagerException {
        castAttributes(crap.getValues());
    }

    /**
     * Package-visible (due to it being embarrassing) method to cast
     * comparables to correct comparable type. Confused? Yeah, you
     * should be.
     *
     * Assumes all comparables are strings and casts them to the
     * correct comparable type. I can't even begin to describe how
     * stupid this is. Actually, I can begin to describe how stupid it
     * is, but I won't finish on time. So, there.
     *
     * No, really. This is stupid. We're talking about a language that
     * after ensuring type-safety at compile-time, it cannot guarantee
     * it at run-time after serialization. This is just insane.
     *
     * @param crap the list of crap you feed the tuple.
     * @throws StorageManagerException when crap smells too bad.
     */
    void castAttributes(List<Comparable> crap) throws StorageManagerException {
        
        for (int i = 0; i < crap.size(); i++) {
            Comparable c = crap.get(i);

            Class<? extends Comparable> type =
                relation.getAttribute(i).getType();            
            if (type.equals(Byte.class))
                crap.set(i, new Byte((String) c));
            else if (type.equals(Short.class))
                crap.set(i, new Short((String) c));
            else if (type.equals(Integer.class))
                crap.set(i, new Integer((String) c));
            else if (type.equals(Long.class))
                crap.set(i, new Long((String) c));
            else if (type.equals(Float.class))
                crap.set(i, new Float((String) c));
            else if (type.equals(Double.class))
                crap.set(i, new Double((String) c));
            else if (type.equals(Character.class))
                crap.set(i, new Character(((String) c).charAt(0)));
            else if (! type.equals(String.class))
                throw new StorageManagerException("Unsupported type: "
                                                  + type + ".");
        }
    }
	
    /**
     * The name of the file for this manager.
     * 
     * @return the name of the file of this manager
     */
    public String getFileName () {
        return filename;
    } // getFileName()


    /**
     * Opens a page iterator over this relation.
     *
     * @return a page iterator over this relation.
     * @throws IOException whenever the iterator cannot be
     * instantiated from disk.
     * @throws StorageManagerException whenever the iterator cannot be
     * created after the file has been loaded.
     */
    public Iterable<Page> pages()
        throws IOException, StorageManagerException {
        
        return new PageIteratorWrapper();
    } // pages()

    /**
     * Opens a tuple iterator over this relation.
     *
     * @return a tuple iterator over this relation.
     * @throws IOException whenever the iterator cannot be
     * instantiated from disk.
     * @throws StorageManagerException whenever the iterator cannot be
     * created after the file has been loaded.
     */
    public Iterable<Tuple> tuples()
        throws IOException, StorageManagerException {
        
        return new TupleIteratorWrapper();
    } // tuples()

    
    /**
     * The basic iterator over pages of this relation.
     */
    class PageIteratorWrapper implements Iterable<Page> {
        /** The current page of the iterator. */
        private Page currentPage;

        /** The number of pages in the relation. */
        private int numPages;

        /** The current page offset. */
        private int pageOffset;

        /**
         * Constructs a new page iterator.
         */
        public PageIteratorWrapper()
            throws IOException, StorageManagerException {

            pageOffset = 0;
            numPages = FileUtil.getNumberOfPages(getFileName());
        } // PageIteratorWrapper()

        /**
         * Returns an iterator over pages.
         *
         * @return the iterator over pages.
         */
        public Iterator<Page> iterator() {
            return new Iterator<Page>() {
                public boolean hasNext() {
                    return pageOffset < numPages;
                } // hasNext()
                public Page next() throws NoSuchElementException {
                    try {
                        currentPage =
                            sm.readPage(relation,
                                        new PageIdentifier(getFileName(),
                                                           pageOffset++));
                        return currentPage;
                    }
                    catch (StorageManagerException sme) {
                        throw new NoSuchElementException("Could not read "
                                                         + "page to advance "
                                                         + "the iterator.");
                                                         
                    }
                } // next()
                public void remove() throws UnsupportedOperationException {
                    throw new UnsupportedOperationException("Cannot remove "
                                                            + "from page "
                                                            + "iterator.");
                } // remove()
            }; // new Iterator
        } // iterator()
    } // PageIteratorWrapper


    /**
     * The basic iterator over tuples of this relation.
     */
    class TupleIteratorWrapper implements Iterable<Tuple> {
        /** The page iterator. */
        private Iterator<Page> pages;
        
        /** The single-page tuple iterator. */
        private Iterator<Tuple> tuples;

        /** Keeps track of whether there are more elements to return. */
        private boolean more;

        /**
         * Constructs a new tuple iterator.
         */
        public TupleIteratorWrapper()
            throws IOException, StorageManagerException {
            
            pages = pages().iterator();
            more = pages.hasNext();
            tuples = null;
        } // TupleIterator()

        /**
         * Checks whether there are more tuples in this iterator.
         *
         * @return <code>true</code> if there are more tuples,
         * <code>false</code> otherwise.
         */
        public Iterator<Tuple> iterator() {
            return new Iterator<Tuple>() {
                public boolean hasNext() {
                    return more;
                } // hasNext()
                public Tuple next() throws NoSuchElementException {
                    if (tuples == null && more)
                        tuples = pages.next().iterator();

                    Tuple tuple = tuples.next();
                    if (tuples.hasNext()) more = true;
                    else if (pages.hasNext()) {
                        tuples = pages.next().iterator();
                        more = true;
                    }
                    else more = false;
                    return tuple;
                } // next()
                public void remove() throws UnsupportedOperationException {
                    throw new UnsupportedOperationException("Cannot remove "
                                                            + "from tuple "
                                                            + "iterator.");
                } // remove()
            }; // new Iterator
        } // iterator()
    } // TupleIteratorWrapper

    
    /**
     * Debug main.
     */
    public static void main (String [] args) {
        try {
            BufferManager bm = new BufferManager(100);
            StorageManager sm = new StorageManager(null, bm);
            
            List<Attribute> attributes = new ArrayList<Attribute>();
            attributes.add(new Attribute("integer", Integer.class));
            attributes.add(new Attribute("string", String.class));
            Relation relation = new Relation(attributes);
            String filename = args[0];
            sm.createFile(filename);
            RelationIOManager manager = 
                new RelationIOManager(sm, relation, filename);
            
            for (int i = 0; i < 30; i++) {
                List<Comparable> v = new ArrayList<Comparable>();
                v.add(new Integer(i));
                v.add(new String("bla"));
                Tuple tuple = new Tuple(new TupleIdentifier(filename, i), v);
                System.out.println("inserting: " + tuple);
                manager.insertTuple(tuple);
            }
            
            System.out.println("Tuples successfully inserted.");
            System.out.println("Opening tuple cursor...");

            for (Tuple tuple : manager.tuples())
                System.out.println("read: " + tuple);
            
        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }	
    } // main()
    
} // RelationIOManager
