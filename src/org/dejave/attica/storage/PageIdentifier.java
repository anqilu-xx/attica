/*
 * Created on Nov 26, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

/**
 * PageIdentifier: Identifies an attica page on disk.
 *
 * @author sviglas
 */
public class PageIdentifier {
	
    /** The file this page belongs to. */
    private String fileName;
	
    /** The number of this page. */
    private int number;

    
    /**
     * Creates a new identifier.
     * 
     * @param fileName the file this page belongs to.
     * @param number the number of this page.
     */
    public PageIdentifier(String fileName, int number) {
        
        this.fileName = fileName;
        this.number = number;
    } // PageIdentifier()

    
    /**
     * Retrieves the name of the file this page belongs to.
     * 
     * @return the name of the file this page belongs to.
     */
    public String getFileName() {
        
        return fileName;
    } // getFileName()

    
    /**
     * Retrieves the number of this page.
     * 
     * @return the number of this page.
     */
    public int getNumber () {
        return number;
    } // getNumber()

    
    /**
     * Compares two identifiers for equality.
     * 
     * @param o the object this identifier is compared to
     * @return <pre>true</pre> if the two identifiers are equal,
     * <pre>false</pre> otherwise
     */
    @Override
    public boolean equals(Object o) {
        
        if (o == this) return true;        
        if (! (o instanceof PageIdentifier)) return false;
        PageIdentifier pi = (PageIdentifier) o;        
        return (getFileName() == null
                ? pi.getFileName() == null
                : getFileName().equals(pi.getFileName()))
            && getNumber() == pi.getNumber();
    } // equals()

    
    /**
     * Overrides hash code for equals() consistency.
     *
     * @return the hash code of this page identifier.
     */
    @Override
    public int hashCode() {

        // just DEK it and be done with it...
        int hash = 17;
        int code = (getFileName() != null ? getFileName().hashCode() : 0);
        hash = 31*hash + code;
        code = getNumber();
        hash = 31*hash + code;
        return hash;
    } // hashCode()

    
    /**
     * Returns a string representation of this identifier.
     * 
     * @return this identifier's string representation.
     */
    @Override
    public String toString() {
        
        return "[page " + getFileName() + ":" + getNumber() + "]";
    } // toString()

} // PageIdentifier
