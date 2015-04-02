/*
 * Created on Dec 2, 2003 by sviglas
 *
 * Modified on Dec 23, 2003 by sviglas
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

import org.dejave.attica.model.Attribute;
import org.dejave.attica.model.Relation;

import org.dejave.util.Convert;
import org.dejave.util.Pair;


/**
 * TupleIOManager: Takes care of tuple input and output.
 *
 * @author sviglas
 */
public class TupleIOManager {
	
    /** The relation tuples belong to. */
    private Relation relation;
	
    /** The filename of this relation. */
    private String filename;
	
    /**
     * Construct a new manager given a relation schema.
     * 
     * @param relation the relation schema.
     * @param filename the name of the relation's file.
     */
    public TupleIOManager(Relation relation, String filename) { 
        this.relation = relation;
        this.filename = filename;
    } // TupleIOManager()

    
    /**
     * Writes a tuple to a byte array -- should be used when there is
     * page buffering.
     * 
     * @param tuple the tuple to be written.
     * @param bytes the byte array the tuple should be written to.
     * @param start the starting offset into the byte array.
     * @return the new offset in the byte array.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public int writeTuple(Tuple tuple, byte [] bytes, int start) 
	throws StorageManagerException {
        
        // write the tuple id
        byte [] b = Convert.toByte(tuple.getTupleIdentifier().getNumber());
        System.arraycopy(b, 0, bytes, start, b.length);
        start += b.length;
        // make an attempt to have the array GC'ed (since Java is
        // brain-damaged and we can't release memory -- what the F
        // were you people thinking?
        //
        // but it's OK, I'm not going to rant about an array of four
        // bytes now; there are plenty more reasons to bash Java for.
        b = null;
        
        // NB slot++ is in the dumpSlot() call -- uberweiss
        int slot = 0;    
        for (Attribute attr : relation)
            start = dumpSlot(attr.getType(), tuple, slot++, bytes, start);

        return start;
    } // writeTuple()

    
    /**
     * Reads a tuple from a byte array -- should be used when there is
     * page buffering.
     * 
     * @param bytes the byte array.
     * @param start the starting offset into the byte array.
     * @return a pair consisting of the tuple read and the new offset
     * in the byte array.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    public Pair<Tuple, Integer> readTuple(byte [] bytes, int start)
        throws StorageManagerException {
        
        // read in the tuple id
        byte [] b = new byte[Convert.INT_SIZE];
        System.arraycopy(bytes, start, b, 0, b.length);
        int id = Convert.toInt(b);
        start += b.length;
        List<Comparable> values = new ArrayList<Comparable>();
        int slot = 0;
        for (Iterator<Attribute> it = relation.iterator();
             it.hasNext(); slot++) {
            Pair<? extends Comparable, Integer> pair =
                fetch(it.next().getType(), bytes, start);
            start = pair.second;
            values.add(pair.first);
        }
        Tuple t = new Tuple(new TupleIdentifier(filename, id), values);
        return new Pair<Tuple, Integer>(t, new Integer(start));
    } // readTuple()

    
    /**
     * Low-level method to dump a tuple slot to a byte array.
     * 
     * @param type the type of the slot.
     * @param t the tuple.
     * @param s the slot of the tuple to be dumped.
     * @param bytes the byte array where the slot will be dumped.
     * @param start the starting offset.
     * @return the new offset in the array.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    protected int dumpSlot(Class<? extends Comparable> type, Tuple t,
                           int s, byte [] bytes, int start) 
	throws StorageManagerException {

        if (type.equals(Character.class)) {
            byte [] b = Convert.toByte(t.asChar(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Byte.class)) {
            bytes[start] = t.asByte(s);
            return start+1;
        }
        else if (type.equals(Short.class)) {
            byte [] b = Convert.toByte(t.asShort(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Integer.class)) {
            byte [] b = Convert.toByte(t.asInt(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Long.class)) {
            byte [] b = Convert.toByte(t.asLong(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Float.class)) {
            byte [] b = Convert.toByte(t.asFloat(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(Double.class)) {
            byte [] b = Convert.toByte(t.asDouble(s));
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else if (type.equals(String.class)) {
            String st = t.asString(s);
            int len = st.length();
            byte [] b = Convert.toByte(len);
            System.arraycopy(b, 0, bytes, start, b.length);
            start += b.length;
            b = Convert.toByte(st);
            System.arraycopy(b, 0, bytes, start, b.length);
            return start + b.length;
        }
        else {
            // this is an unsupported type for the time being, so
            // throw an exception
            throw new StorageManagerException("Unsupported type when "
                                              + "writing tuple: "
                                              + type.getClass().getName()
                                              + ".");
        }
    } // dumpSlot()
    
    
    /**
     * Low-level method to read a slot from a byte array.
     * 
     * @param type the type of the slot to be read.
     * @param bytes the input byte array.
     * @param start the starting offset in the byte array.
     * @return a pair of (value, offset) with the value read and the
     * new offset in the byte array.
     * @throws StorageManagerException thrown whenever there is an I/O
     * error.
     */
    protected Pair<? extends Comparable, Integer>
        fetch(Class<? extends Comparable> type, byte [] bytes, int start)
	throws StorageManagerException {
        
        try {
            if (type.equals(Character.class)) {
                byte [] b = new byte[Convert.CHAR_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Character, Integer>(
                    new Character(Convert.toChar(b)), 
                    start + b.length);
            }
            else if (type.equals(Byte.class)) {
                return new Pair<Byte, Integer>(bytes[start], 
                                               start + 1);
            }
            else if (type.equals(Short.class)) {
                byte [] b = new byte[Convert.SHORT_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Short, Integer>(new Short(Convert.toShort(b)),
                                                start + b.length);
            }
            else if (type.equals(Integer.class)) {
                byte [] b = new byte[Convert.INT_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Integer, Integer>(new Integer(Convert.toInt(b)),
                                                  start + b.length);
            }
            else if (type.equals(Long.class)) {
                byte [] b = new byte[Convert.LONG_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Long, Integer>(new Long(Convert.toLong(b)),
                                               start + b.length);
            }
            else if (type.equals(Float.class)) {
                byte [] b = new byte[Convert.FLOAT_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Float, Integer>(new Float(Convert.toFloat(b)),
                                                start + b.length);
            }
            else if (type.equals(Double.class)) {
                byte [] b = new byte[Convert.DOUBLE_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                return new Pair<Double, Integer>(
                    new Double(Convert.toDouble(b)),
                    start + b.length);
            }
            else if (type.equals(String.class)) {
                byte [] b = new byte[Convert.INT_SIZE];
                System.arraycopy(bytes, start, b, 0, b.length);
                start += b.length;
                int stLength = Convert.toInt(b);
                b = new byte[2*stLength];
                System.arraycopy(bytes, start, b, 0, b.length);
                String str = Convert.toString(b);
                return new Pair<String, Integer>(str, start + b.length);
            }
            else {
                throw new StorageManagerException("Unsupported type: "
                                                  + type.getClass().getName()
                                                  + ".");
            }
        }
        catch (ArrayIndexOutOfBoundsException aiob) {
            throw new StorageManagerException("Generic error while reading " +
                                              "table row (boundary error.)",
                                              aiob);
        }
    } // fetch()


    /**
     * Calculates the byte size of a tuple of this IO manager.
     *
     * @param t the tuple.
     * @return the byte size of the tuple.
     */
    public int byteSize(Tuple t) {
        return byteSize(relation, t);
    }
    
    /**
     * Calculates the byte size of a tuple given its relation. (Lame,
     * lame, lame... I should hang my head in shame. Or go play a
     * game. Or see if I can start a big fire with a small flame. Or
     * stop looking for words that rhyme with lame.)     
     *
     * @param rel the relation.
     * @param t the tuple.
     * @return the size in bytes of tuple.
     */
    public static int byteSize(Relation rel, Tuple t) {
        // one long for the id
        int size = Convert.INT_SIZE;
	int slot = 0;
        for (Attribute it : rel) {
            Class<?> type = it.getType();
            if (type.equals(Character.class)) size += Convert.CHAR_SIZE;
            else if (type.equals(Byte.class)) size += 1;
            else if (type.equals(Short.class)) size += Convert.SHORT_SIZE;
            else if (type.equals(Integer.class)) size += Convert.INT_SIZE;
            else if (type.equals(Long.class)) size += Convert.LONG_SIZE;
            else if (type.equals(Float.class)) size += Convert.FLOAT_SIZE;
            else if (type.equals(Double.class)) size += Convert.DOUBLE_SIZE;
            else if (type.equals(String.class))
                size += Convert.INT_SIZE + 2 * t.asString(slot).length();

            slot++;
        }
	
        return size;
    } // byteSize()

    
    /**
     * Debug main.
     */
    /*
    public static void main (String args []) {
        try {
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
            Tuple t = new Tuple(new TupleIdentifier(args[0], 0), v);
            TupleIOManager man = new TupleIOManager(rel, args[0]);
            
            java.io.RandomAccessFile raf = new java.io.RandomAccessFile(args[0], "rw");
            
            System.out.println("writing tuple...");
            man.writeTuple(t, raf);
            raf.close();
            
            raf = new java.io.RandomAccessFile(args[0], "r");
            System.out.println("reading tuple...");
            t = man.readTuple(raf);
            System.out.println(t);
            raf.close();
        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()
    */
    
} // TupleIOManager
