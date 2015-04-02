/*
 * Created on Jan 12, 2015 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.server;

/**
 * EnableHash: Enables or disables hash algorithms.
 *
 * @author sviglas
 */
public class EnableHash extends Statement {

    private boolean enabled;
	
    /**
     * Default constructor.
     */
    public EnableHash () {
	this(true);
    } // ShowCatalog()

    /**
     * Enables or disables hashing.
     */
    public EnableHash(boolean e) {
	enabled = e;
    } // EnableHash()

    public boolean isEnabled() {
	return enabled;
    }

    public boolean isDisabled() {
	return !enabled;
    }

} // ShowCatalog
