/*
 * Created on Jan 4, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.util.Date;

/**
 * Wrapper for a page in the buffer pool, keeping track of its
 * dirtyness.
 *
 * @author sviglas
 */
class BufferedPage {

    public Page page;
    public boolean dirty;

    public BufferedPage(Page page) {
        this.page = page;
        dirty = false;
    } // BufferedPage()

    
} // BufferedPage
