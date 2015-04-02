/*
 * Created on Oct 9, 2003 by sviglas
 *
 * Modified on Dec 18, 2008 by sviglas
 * 
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.dejave.util.Convert;
import org.dejave.attica.model.Relation;

/**
 * Page: The basic representation of an attica page. NB: this is the
 * language representation -- disk pages will likely vary.
 *
 * @author sviglas
 */
public class Page implements Iterable<Tuple> {
    
    /** The tuples of this page. */
    private List<Tuple> tuples;
	
    /** The ID of the page. */
    private PageIdentifier pageId;
	
    /** The relational schema this page conforms to. */
    private Relation relation;

    /** The free space in this page. */
    private int freeSpace;
    
    /**
     * Creates a new page given its schema and page identifier.
     * 
     * @param relation the relation this page conforms to.
     * @param pageId the ID of this page.
     */
    public Page(Relation relation, PageIdentifier pageId) {
        this.relation = relation;
        this.pageId = pageId;
        this.tuples = new ArrayList<Tuple>();
        freeSpace = Sizes.PAGE_SIZE - Convert.INT_SIZE;
    } // Page()
	

    /**
     * Returns the relation this page conforms to.
     * 
     * @return the relation this page belongs to.
     */
    public Relation getRelation() {
        return relation;
    } // getRelation()

    
    /**
     * Checks whether this page has room for one more tuple.
     *
     * @return <pre>true</pre> if there is room for one more tuple
     * on this page, <pre>false</pre> otherwise.
     */
    public boolean hasRoom(Tuple t) {
        return freeSpace >= TupleIOManager.byteSize(getRelation(), t);
    } // hasSpace()

    
    /**
     * Returns the number of occupied tuples of this page.
     *
     * @return the number of occupied tuples of this page.
     */
    public int getNumberOfTuples() {
        return tuples.size();
    } // getNumberOfTuples()

    
    /**
     * Sets the number of tuples in this page (package visible).
     *
     * @param numTuples the new number of tuples.     
     */
    /*
    void setNumberOfTuples(int numTuples) {
        this.numTuples = numTuples;
    } // setNumberOfTuples
    */
    
    /**
     * Adds a new tuple to the page.
     * 
     * @param tuple the new tuple.
     * @throws ArrayIndexOutOfBoundsException thrown whenever the page
     * boundaries are crossed.
     */
    public void addTuple(Tuple tuple) 
	throws ArrayIndexOutOfBoundsException {
        
        if (hasRoom(tuple)) {
            tuples.add(tuple);
            freeSpace -= TupleIOManager.byteSize(getRelation(), tuple);
        }
        else throw new ArrayIndexOutOfBoundsException("No more space in page.");
    } // addTupleToPage()

    
    /**
     * Sets the specified tuple.
     * 
     * @param index the index of the tuple to be changed.
     * @param tuple the new tuple.
     * @throws ArrayIndexOutOfBoundsException thrown whenever the page
     * boundaries are crossed.
     * @throws IllegalArgumentException if the new tuple does not fit
     * into the page.
     */
    public void setTuple(int index, Tuple tuple) 
	throws ArrayIndexOutOfBoundsException, IllegalArgumentException {

        if (! canSubstitute(index, tuple))
            throw new IllegalArgumentException("New tuple does not fit.");
        tuples.set(index, tuple);
    } // setTuple()


    /**
     * Checks whether the specified index can be substituted for the
     * new tuple.
     *
     * @param index the index of the tuple to be changed.
     * @param nt the new tuple.
     * @throws ArrayIndexOutOfBoundsException whenever the referenced
     * index does not exist.
     */
    public boolean canSubstitute(int index, Tuple nt)
        throws ArrayIndexOutOfBoundsException {

        return (freeSpace
            + TupleIOManager.byteSize(getRelation(), tuples.get(index))
                - TupleIOManager.byteSize(getRelation(), nt)) >= 0;
    } // canSubstitute()


    /**
     * Swaps two tuples by their indexes.
     *
     * @param x the first index.
     * @param y the second index.
     * @throws ArrayIndexOutOfBoundsException whenever any of the
     * referenced indexes do not exist.
     */
    public void swap(int x, int y) throws ArrayIndexOutOfBoundsException {
        Tuple t = tuples.get(x);
        tuples.set(x, tuples.get(y));
        tuples.set(y, t);
    } // swap()

    
    /**
     * Retrieves the specified tuple from the page.
     * 
     * @param index the index of the tuple to be retrieved.
     * @return the index-th tuple.
     * @throws ArrayIndexOutOfBoundsException thrown whenever there
     * is an error in the indexing.
     */
    public Tuple retrieveTuple(int index) 
	throws ArrayIndexOutOfBoundsException {
        
        return tuples.get(index);
    } // retrieveTuple()

    
    /**
     * Retrieves the ID of this page
     * 
     * @return the ID of the page
     */
    public PageIdentifier getPageIdentifier() {        
        return pageId;
    } // getPageIdentifier()

    
    /**
     * Returns an iterator over this page.
     *
     * @return an iterator over the tuples of this page.
     */
    public Iterator<Tuple> iterator() {
        return new PageIterator();
    }

    /**
     * The iterator over the tuples of this page. Doesn't wrap the
     * list iterator because we want to keep track of free space on
     * removal (and, yes, this is brain-damaged since the system
     * doesn't support deletions yet).
     */
    private class PageIterator implements Iterator<Tuple> {
        /** The current index of the iterator. */
        private int currentIndex;

        /**
         * Constructs a new iterator over this page's contents.
         */
        public PageIterator() {
            currentIndex = 0;
        }

        /**
         * Checks whether there are more tuples in this page.
         *
         * @return <code>true</code> if there are more tuples in this
         * page, <code>false</code> otherwise.
         */
        public boolean hasNext() {
            return currentIndex < tuples.size();
        }

        /**
         * Returns the next tuple of the iterator.
         *
         * @return the next tuple of the iterator.
         */
        public Tuple next() {
            return tuples.get(currentIndex++);
        }

        /**
         * Removes the last tuple returned by the iterator.
         */
        public void remove() {
            int size = TupleIOManager.byteSize(getRelation(),
                                               tuples.get(currentIndex));
            freeSpace += size;
            tuples.remove(currentIndex);
        }
    } // PageIterator()
    

    /**
     * Returns a textual representation of the page.
     * 
     * @return this page's textual representation.
     */
    @Override
    public String toString() {
        
        StringBuffer sb = new StringBuffer();
        sb.append("page: " + getPageIdentifier() + ", tuples: {\n");
        int tid = 0;
        for (Tuple it : this)
            sb.append("\t" + tid++ + ": " + it.toString() + "\n");
        sb.append("}");
        return sb.toString();
    } // toString()
    
} // Page
