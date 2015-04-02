package org.dejave.util;

/**
 * @author sviglas
 *
 * Utility class that wraps a pair of values.
 */

public class Pair <F, S> {
    /** The first element. */
    public F first;
    /** The second element. */
    public S second;


    /**
     * Default constructor.
     */
    public Pair() {
        first = null;
        second = null;
    }

    /**
     * Constructs a new pair given its first and second elements.
     *
     * @param f the first element.
     * @param s the second element.
     */
    public Pair(F f, S s) {
        first = f;
        second = s;
    } // Pair()


    /**
     * Tests this pair for equality to an object.
     *
     * @param o the object to test for equality with.
     * @return <code>true</code> if <code>this</code> is equal to
     * <code>o</code>, <code>false</code> otherwise.
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        if (o == this) return true;
        if (! (o instanceof Pair)) return false;
        try {
            Pair<F, S> p = (Pair<F, S>) o;
            return (first == null
                    ? p.first == null : first.equals(p.first))
                && (second == null
                    ? p.second == null : second.equals(p.second));
        }
        catch (ClassCastException cce) {
            return false;
        }
    } // equals()


    /**
     * Computes a hashcode for this pair.
     *
     * @return this pair's hashcode.
     */
    @Override
    public int hashCode() {
        int hash = 17;
        int code = (first != null ? first.hashCode() : 0);
        hash = hash*31 + code;
        code = (second != null ? second.hashCode() : 0);
        return hash*31 + code;
    } // hashCode()

    /**
     * Textual representation.
     *
     * @return this pair's textual representation.
     */
    @Override
    public String toString() {
        return "(" + (first != null ? first : ("null"))
            + ", " + (second != null ? second : ("null")) + ")";
    } // toString()
}
