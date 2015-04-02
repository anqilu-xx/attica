package org.dejave.util;

/**
 * Convenience methods for parsing command-line arguments.
 *
 * @author sviglas
 */

public class Args {

    /**
     * Return true of false depending on whether the given argument
     * is present or not.
     *
     * @param args the arguments.
     * @param trig the argument to be looked for.
     * @return true of the argument is there, false otherwise.
     */
    public static boolean gettrig(String [] args, String trig) {
        for (int i = 0; i < args.length; i++)
            if (args[i].equals(trig)) return true;
        
        return false;
    } // gettrig()
    
    /**
     * Given command line arguments as an array of strings, get the value
     * for an optional argument, null if it's not there.
     *
     * @param args the arguments.
     * @param opt the optional argument to be retrieved.
     * @return the value of the optional argument, or <code>null</code>
     * if the argument is not present.
     */
    public static String getopt(String [] args, String opt) {
        for (int i = 0; i < args.length; i++)
            if (args[i].equals(opt))
                return args[i+1];
        
        return null;
    } // getopt()
	
    /**
     * Given command line arguments as an array of strings, get the value
     * for an optional argument, and a default value if it is not there.
     *
     * @param args the arguments.
     * @param opt the optional argument to be retrieved.
     * @param def the default value.
     * @return the value of the optional argument, or the default value
     * if the argument is not present.
     */
    public static String getopt(String [] args, String opt, String def) {
        String val = getopt(args, opt);
        return (val == null ? def : val);
    } // getopt()
    
} // Args
