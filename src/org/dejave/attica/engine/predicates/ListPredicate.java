/*
 * Created on Dec 11, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.predicates;

import java.util.List;

/**
 * ListPredicate: A predicate over a list of predicates.
 *
 * @author sviglas
 */
public abstract class ListPredicate {
	
    /** The predicate list. */
    private List<Predicate> list;
	
    /**
     * Constructs a new list predicate.
     * 
     * @param list the predicate list.
     */
    public ListPredicate(List<Predicate> list) {
        this.list = list;
    } // ListPredicate()

    
    /**
     * Returns the length of the predicate list.
     * 
     * @return the predicate list's length
     */
    public int getPredicateListLength() {
        return list.size();
    } // getPredicateListLength()

    
    /**
     * Returns the requested predicate.
     * 
     * @param i the index of the predicate to be retrieved.
     * @return the index-th predicate.
     */
    public Predicate getPredicate(int i) {
        return list.get(i);
    } // getPredicate()

    
    /**
     * Sets the given predicate of the predicate list.
     * 
     * @param i the index of the predicate to be set.
     * @param predicate the new predicate.
     */
    public void setPredicate(int i, Predicate predicate) {
        list.set(i, predicate);
    } // setPredicate()


    /**
     * Returns the iterable list of predicates.
     *
     * @return the iterable list of predicate.
     */
    public Iterable<Predicate> predicates() {
        return list;
    } // getList()
    
    
    /**
     * Returns the symbol of this list.
     * 
     * @return this list's symbol.
     */
    protected String listSymbol() {
        return "??";
    } // listSymbol()

    
    /**
     * Textual representation.
     * 
     * @return this conjunction's textual representation.
     */
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("((" + getPredicate(0) +")");
        for (int i = 1; i < getPredicateListLength(); i++) {
            sb.append(listSymbol() + "(" + getPredicate(i) +")");
        }
        sb.append(")");
        return sb.toString();
    } // toString()

} // ListPredicate
