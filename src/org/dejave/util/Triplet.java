package org.dejave.util;

public class Triplet <F, S, T> {
    public F first;
    public S second;
    public T third;

    public Triplet() { this(null, null, null); }
    
    public Triplet(F f, S s, T t) {
        first = f;
        second = s;
	third = t;
    }
    
    public String toString() {
	return "(" + first + ", " + second + ", " + third + ")";
    }
}
