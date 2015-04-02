package org.dejave.util;

import java.util.logging.Handler;
import java.util.logging.Formatter;
import java.util.logging.Level;


/**
 * Does the boilerplate work for loggers.
 *
 * @author Stratis Viglas &lt;org.dejave.glas@inf.ed.ac.uk&gt;
 * @version $0.1$
 */
public final class Logger {
    
    public static java.util.logging.Logger createLogger(String n,
                                                        Handler h,
                                                        Formatter f) {
        h.setFormatter(f);
        return createLogger(n, h);
    }
    
    public static java.util.logging.Logger createLogger(String n, Handler r) {
        java.util.logging.Logger logger = 
            java.util.logging.Logger.getLogger(n);
        Handler [] handlers = logger.getHandlers();
        for (int i = 0; i < handlers.length; i++)
            logger.removeHandler(handlers[i]);
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.INFO);
        logger.addHandler(r);
        return logger;
    }
    
}
