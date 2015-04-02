/*
 * Created on Nov 25, 2003 by sviglas
 *
 * Modified on Dec 20, 2008 by sviglas
 *
 * Heavily modified on Jan 4, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.storage;

import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;


/**
 * BufferManager: The basic abstraction of attica's buffer manager.
 *
 * @author sviglas
 */
public class BufferManager {
	
    /** The number of pages stored in this buffer manager. */
    private int numPages;
	
    /** The pages stored in this buffer manager. */
    private BufferedPage [] pages;
	
    /** Maps a page id to an index in the page array. */
    private Map<PageIdentifier, Integer> idToIdx;

    /** The current index in the pool. */
    private int currentIndex;

    /** The LRU replacement queue. */
    private LinkedList<PageIdentifier> lruQueue;
    
    /**
     * Create a new buffer manager given the number of pages the
     * buffer manager should hold.
     * 
     * @param numPages this buffer manager's number of pages
     */
    public BufferManager(int numPages) {
        
        this.numPages = numPages;
        currentIndex = 0;
        pages = new BufferedPage[numPages];
        idToIdx = new HashMap<PageIdentifier, Integer>();
        // you may be asking: are you stupid enough to organise the
        // LRU queue on identifiers rather than indexes? well, I'm
        // not. but java is stupid enough to autobox/unbox all
        // integers in a generic list of integers and this may lead to
        // some problems, to put it mildly (since there is no decent
        // queue implementation, and we're actually using a linked
        // list)
        // 
        lruQueue = new LinkedList<PageIdentifier>();
    } // BufferManager()

    
    /**
     * The number of pages stored in this buffer manager.
     * 
     * @return this buffer manager's number of pages.
     */
    public int getNumberOfPages() {
        return numPages;
    } // getNumberOfPages()

    
    /**
     * Is the page specified by the page id in the buffer pool
     * or not?
     * 
     * @param pageid the id of the page searched for.
     * @return <pre>true</pre> if the page is in the buffer pool
     * <pre>false</pre> otherwise.
     */
    public boolean containsPage(PageIdentifier pageid) {
        return getIndex(pageid) != -1;
    } // containsPage()

    
    /**
     * Returns a page given a page id.
     * 
     * @param pageid the id of the page to be returned
     * @return the page that corresponds to the given page id
     */
    public Page getPage(PageIdentifier pageid) {
        
        int index = getIndex(pageid);
        if (index >= 0) {
            lruQueue.remove(pageid);
            lruQueue.add(pageid);
            return pages[index].page;
        }
        return null;
    } // getPage()


    /**
     * Returns the index of a page in the pool given its identifier.
     *
     * @return the index of a page in the pool, or -1 if the page is
     * not there
     */
    protected int getIndex(PageIdentifier pageid) {
        
        Integer idx = idToIdx.get(pageid);
        return (idx == null ? -1 : idx);
    } // getIndex()

    
    /**
     * Touches the page corresponding to the pageid making it dirty
     * and moving it to the back of the replacement queue.
     * 
     * @param pageid the pageid of the page to be touched.
     */
    public void touchPage(PageIdentifier pageid) { 

        if (containsPage(pageid)) {
            int index = getIndex(pageid);
            pages[index].dirty = true;
            lruQueue.remove(pages[index].page.getPageIdentifier());
            lruQueue.add(pages[index].page.getPageIdentifier());
        }
    } // touchPage()

    
    /**
     * Touches a page in the buffer pool (if it exists) making it dirty
     * and setting its timestamp.
     * 
     * @param page the page to be touched.
     */
    public void touchPage(Page page) {
        touchPage(page.getPageIdentifier());
    } // touchPage()


    /**
     * Wrapper for putting a page in the buffer pool, assuming the
     * page is dirty.
     *
     * @param page the page to be inserted.
     * @return the page to be evicted (if any).
     */
    public Page putPage(Page page) {
        return putPage(page, true);
    } // putPage()
    
    
    /**
     * Puts a page in the buffer pool.
     * 
     * @param page the page to be inserted into the buffer pool.
     * @param dirty is this page dirty or not?
     * @return the page to be evicted (if any).
     */
    public Page putPage(Page page, boolean dirty) {
        
        int index; 
        // if the page is in the buffer pool
        if (containsPage(page.getPageIdentifier())) {
            index = getIndex(page.getPageIdentifier());
            pages[index].page = page;
            pages[index].dirty = dirty;
            lruQueue.remove(page.getPageIdentifier());
            lruQueue.add(page.getPageIdentifier());
            return null;
        }
        // if the page is not in the buffer pool, but the buffer
        // pool is not full
        else if (! isFull()) {
            index = currentIndex++;
            idToIdx.put(page.getPageIdentifier(), index);
            pages[index] = new BufferedPage(page);
            pages[index].dirty = dirty;
            lruQueue.add(page.getPageIdentifier());
            return null;
        }
        // if the page is not in the buffer pool and the buffer
        // pool is full
        else {
            index = indexToEvict();
            Page pageToFlush = pages[index].page;
            idToIdx.remove(pageToFlush.getPageIdentifier());
            idToIdx.put(page.getPageIdentifier(), index);
            pages[index].dirty = dirty;
            pages[index].page = page;
            lruQueue.add(page.getPageIdentifier());
            return pageToFlush;
        }
    } // putPage()

    
    /**
     * Is the buffer manager full or not?
     * 
     * @return <pre>true</pre> if the buffer manager is full
     * <pre>false</pre> otherwise.
     */
    protected boolean isFull() {
        return currentIndex == numPages;
    } // isFull()

    
    /**
     * Identify the page to be evicted from the buffer pool to make
     * room.
     * 
     * @return the index of the page that should be evicted.
     */
    protected int indexToEvict() {
        PageIdentifier pid = lruQueue.removeFirst();
        return idToIdx.get(pid);
        /*
         //
         // left here for posterity as a token of everlasting
         // stupidity
         //
        int index = 0;
        Date latest = pages[0].timestamp;
        int i = 0;
        for (BufferedPage page : pages) {
            if (page.timestamp.compareTo(latest) < 0) {
                index = i;
                latest = page.timestamp;
            }
            i++;
        }	
        return index;
        */
    } // indexToEvict()


    /**
     * Invalidate all buffer pool entries for a specific file.
     *
     * @param fn the filename.     
     */
    public void invalidate(String fn) {
        
        for (int index = 0; index < currentIndex;) {
            PageIdentifier pid = pages[index].page.getPageIdentifier();
            if (pid.getFileName().equals(fn)) {
                System.arraycopy(pages, index+1, pages, index,
                                 currentIndex-index-1);
                idToIdx.remove(pid);
                lruQueue.remove(pid);
                // update all page references in the id to index map
                for (Map.Entry<PageIdentifier, Integer> e :
                         idToIdx.entrySet()) {
                    // autoboxing doesn't always work, so just autobox
                    // it before access, just in case
                    int v = e.getValue();
                    if (v > index) e.setValue(v-1);
                }
                currentIndex--;
            }
            else {
                index++;
            }
        }
    } // invalidate()

    
    /**
     * Returns an iterable over the pages of the buffer pool.
     *
     * @return an iterable over the pages of the buffer pool.
     */
    Iterable<Page> pages() {
        return new PageIteratorWrapper();
    } // pages()


    /**
     * Inner iterator for the pages of the buffer pool.
     */
    class PageIteratorWrapper implements Iterable<Page> {
        private int index;
        public PageIteratorWrapper() { index = 0; }

        public Iterator<Page> iterator() {
            return new Iterator<Page>() {
                public boolean hasNext() { return index < currentIndex; }
                public Page next() { return pages[index++].page; }
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            }; // new Iterator
        } // iterator()
    } // PageIteratorWrapper

} // BufferManager
