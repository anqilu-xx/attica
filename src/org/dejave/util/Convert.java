/*
 * Created on Dec 4, 2003 by org.dejave.glas
 *
 * This is part of the attica project.  Any subsequenct modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.util;

/**
 * @author org.dejave.glas
 *
 * Convert: Conversions between primitive types and byte arrays
 */
public class Convert {

    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int SHORT_SIZE = 2;
    public static final int CHAR_SIZE = 2;
    public static final int FLOAT_SIZE = 4;
    public static final int DOUBLE_SIZE = 8;
    
    public Convert() {}

    public static final void toByte(int i, byte [] bytes, int o) {
        for (byte b = 0; b <= 3; b++)
            bytes[o+b] = (byte) (i >>> (3 - b)*8);
    }
    
    public static final byte [] toByte(int i) {
        byte b[] = new byte[4];
        toByte(i, b, 0);
        return b;
    }

    public static final void toByte(short w, byte [] bytes, int o) {
        for (byte b = 0; b <= 1; b++)
            bytes[o+b] = (byte) (w >>> (1 - b)*8);
    }

    public static final byte [] toByte(short w) {
        byte b[] = new byte[2];
        toByte(w, b, 0);
        return b;
    } // toByte()

    public static final void toByte(long l, byte [] bytes, int o) {
        for (byte b = 0; b <= 7; b++)
            bytes[o+b] = (byte)(int) (l >>> (7 - b)*8);
    }

    public static final byte [] toByte (long l) {
        byte b[] = new byte[8];
        toByte(l, b, 0);
        return b;
    } // toByte()

    public static final void toByte(char c, byte [] bytes, int o) {
        for (byte b = 0; b <= 1; b++)
            bytes[o+b] = (byte) (c >>> (1-b)*8);
    }

    public static final byte [] toByte (char c) {
        byte b[] = new byte[2];
        toByte(c, b, 0);
        return b;
    }

    public static void toByte(float f, byte [] bytes, int o) {
        int i = Float.floatToIntBits(f);
        toByte(i, bytes, o);
    }

    public static final byte [] toByte (float f) {  
        byte b[] = new byte[4];
        toByte(f, b, 0);
        return b;
    }

    public static void toByte(double d, byte [] bytes, int o) {
        long l = Double.doubleToLongBits(d);
        toByte(l, bytes, o);
    }

    public static final byte [] toByte (double d) {
        byte [] b = new byte[8];
        toByte(d, b, 0);
        return b;
    }

    public static final byte [] toByte(String s) {
        byte [] b = new byte[2*s.length()];
        toByte(s, b, 0);
        return b;
    }

    public static final byte [] toByte(String s, byte [] bytes, int o) {
        int length = s.length();
        for (int i = 0; i < length; i++) {
            byte [] two = toByte(s.charAt(i));
            bytes[2*i] = two[0];
            bytes[2*i+1] = two[1];
        }
        return bytes;
    }

    public static final int toInt (byte b[]) {
        return toInt(b, 0);
    }

    public static final int toInt (byte b[], int o) {
        int i = 0;
        for (int b0 = 0; b0 <= 3; b0++) {
            int j;
            if (b[o+b0] < 0) {
                j = (byte) (b[o+b0] & 0x7f);
                j |= 0x80;
            }
            else {
                j = b[o+b0];
            }
            i |= j;
            if (b0 < 3) i <<= 8;
        }
        return i;
    }

    public static final short toShort(byte b[]) {
        return toShort(b, 0);
    }
    
    public static final short toShort(byte b[], int o) {
        short word0 = 0;
        for (int b0 = 0; b0 <= 1; b0++) {
            short word1;
            if (b[o+b0] < 0) {
                word1 = (byte)(b[o+b0] & 0x7f);
                word1 |= 0x80;
            }
            else {
                word1 = b[o+b0];
            }
            word0 |= word1;
            if(b0 < 1) word0 <<= 8;
        }
        return word0;
    }
    
    public static final long toLong (byte b[]) {
        return toLong(b, 0);
    }

    public static final long toLong (byte b[], int o) {
        long l = 0L;
        for (int b0 = 0; b0 <= 7; b0++) {
            long l1;
            if (b[o+b0] < 0) {
                l1 = (byte)(b[o+b0] & 0x7f);
                l1 |= 128L;
            }
            else {
                l1 = b[o+b0];
            }
            l |= l1;
            if(b0 < 7) l <<= 8;
        }
        return l;
    }
    
    public static final char toChar (byte b[]) {
        return toChar(b, 0);
    }
    
    public static final char toChar (byte b[], int o) {
        char c = 0;
        c = (char)((c | (char)b[o]) << 8);
        c |= (char)b[o+1];
        return c;
    }
    
    public static final float toFloat (byte b[]) {
        return toFloat(b, 0);
    }
    
    public static final float toFloat (byte b[], int o) {
        float f = 0.0F;
        Float float1 = new Float(f);
        int i = toInt(b, o);
        f = Float.intBitsToFloat(i);
        return f;
    }
    
    
    public static final double toDouble (byte b[]) {
        return toDouble(b, 0);
    }
    
    public static final double toDouble (byte b[], int o) {
        double d = 0.0D;
        Double double1 = new Double(d);
        long l = toLong(b, o);
        d = Double.longBitsToDouble(l);
        return d;
    }

    public static final String toString(byte [] b) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < b.length-1; i = i+2) {
            byte [] two = new byte[2];
            two[0] = b[i];
            two[1] = b[i+1];
            sb.append(toChar(two));
        }
        return sb.toString();
    }
    
    public static void main (String args[]) {
    
        byte b[] = new byte[Convert.LONG_SIZE*2560];
        for (int i = 0; i < 2560; i++)
            Convert.toByte((long) i, b, i*Convert.LONG_SIZE);
            
        for (int i = 0; i < 2560; i++)
            System.out.println(Convert.toLong(b, i*Convert.LONG_SIZE));
            
        for (int i = 0; i < 2560; i++)
            System.out.println(Convert.toLong(b, i*Convert.LONG_SIZE));
    
        /*
        byte b[] = new byte[8];
        //b = Convert.toByte(0x47851f79);
        Convert.toByte(129, b, 0);
        System.out.println("int: " + 0x47851f79);
        for (int i = 0; i <= 3; i++)
            System.out.print("   " + b[i]);
        
        System.out.println();
        System.out.println("back to int: " + Convert.toInt(b));
        System.out.println();
        b = Convert.toByte((short)-177);
        System.out.println("short: " + -177);
        for (int j = 0; j <= 1; j++)
            System.out.print("   " + b[j]);
        
        System.out.println();
        System.out.println("back to short: " + Convert.toShort(b));
        System.out.println();
        b = Convert.toByte(0x48a749338441e818L);
        System.out.println("long: " + 0x48a749338441e818L);
        for (int k = 0; k <= 7; k++)
            System.out.print("   " + b[k]);
        
        System.out.println();
        System.out.println("back to long: " + Convert.toLong(b));
        System.out.println();
        b = Convert.toByte('k');
        System.out.println("char: " + 'k');
        for (int l = 0; l <= 1; l++)
            System.out.print("   " + b[l]);
        
        System.out.println();
        System.out.println("back to char: " + Convert.toChar(b));
        System.out.println();
        b = Convert.toByte(-564351.4F);
        System.out.println("float: " + -564351.4F);
        for (int i1 = 0; i1 <= 3; i1++)
            System.out.print("   " + b[i1]);
        
        System.out.println();
        System.out.println("back to float: " + Convert.toFloat(b));
        System.out.println();
        b = Convert.toByte(139245812345123.45D);
        System.out.println("double: " + 139245812345123.45D);
        for (int j1 = 0; j1 <= 7; j1++)
            System.out.print("   " + b[j1]);
        
        System.out.println();
        System.out.println("back to double: " + Convert.toDouble(b));
        */
    }
} // Convert //:~
