/*
 * Created on Dec 7, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

/**
 * TupleIdentifier: An identifier for an attica tuple.
 *
 * @author sviglas
 */
public class TupleIdentifier {
	
    /** The filename this tuple belongs to. */
    private String filename;
	
    /** The number of this tuple. */
    private int number;

    
    /**
     * Constructs a new tuple identifier given just the name of the file
     * the tuple belongs to -- the number will be set later.
     * 
     * @param filename the name of the file this tuple belongs to.
     */
    public TupleIdentifier(String filename) {
        this(filename, -1);
    } // TupleIdentifier()

    
    /**
     * Constructs a new tuple given the filename it belongs to and its
     * number in that file.
     * 
     * @param filename the filename this tuple belongs to.
     * @param number the number of this tuple.
     */
    public TupleIdentifier(String filename, int number) {
        this.filename = filename;
        this.number = number;
    } // TupleIdentifier()

    
    /**
     * Returns the name of the file this tuple belongs to.
     * 
     * @return the name of the file this tuple belongs to.
     */
    public String getFileName() {
        return filename;
    } // getFileName()

    
    /**
     * Returns the number of this tuple in the file.
     * 
     * @return the number of this tuple in the file.
     */
    public int getNumber() {
        return number;
    } // getNumber()

    
    /**
     * Sets the number of this tuple in the file.
     * 
     * @param number the new number of this tuple in the file.
     */
    public void setNumber(int number){
        this.number = number;
    } // setNumber()

    
    /**
     * Checks two tuple identifiers for equality.
     * 
     * @param o an object to compare this identifier to.
     * @return <pre>true</pre> if the two tuple identifiers are equal
     * <pre>false</pre> otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (! (o instanceof TupleIdentifier)) return false;
        TupleIdentifier tid = (TupleIdentifier) o;
        return getFileName().equals(tid.getFileName()) &&
            getNumber() == tid.getNumber();
    } // equals()

    /**
     * Computes the hashcode of this tuple identifier.
     *
     * @return this tuple identifier's hashcode.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        hash = hash*31 + getFileName().hashCode();
        return hash*31 + getNumber();
    } // hashCode()

    /**
     * Textual representation.
     */
    @Override
    public String toString () {
        return "[" + (getFileName() != null ? getFileName() : "")
            + " - " + getNumber() + "]";
    } // toString()

} // TupleIdentifier
