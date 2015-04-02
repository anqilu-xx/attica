/*
 * Created on Jan 18, 2004 by sviglas
 *
 * This is part of the attica project.  Any subsequenct modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.FileNotFoundException;
import java.io.IOException;


import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.DatabaseFile;
import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.PageIOManager;
import org.dejave.attica.storage.PageIdentifier;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.FileUtil;

import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;


/**
 * @author sviglas
 *
 * ExternalMergeSort: External sort implementation using heapsort.
 */
public class ExternalMergeSort extends UnaryOperator {
    /**
     * The storage manager for the sink
     */
    private StorageManager sm;
    
    /**
     * The name of the temporary file for the output
     */
    private String outputFile;  
    
    /**
     * The manager that undertakes relation I/O
     */
    private RelationIOManager outputMan;

    
    /**
     * The slots that act as the key of the sort
     */ 
    private int [] slots;
    
    /**
     * Number of buffers (i.e., buffer pool pages and 
     * output files) to be used for the sort
     */
    private int buffers;
    
    /** 
     * Iterator over the output file 
     */
    private Iterator<Tuple> outputTuples;

    /** 
     * Reusable tuple list for returns 
     */
    private ArrayList<Tuple> returnList;

    /**
     * Compare two tuples using the specified keys
     */
    @SuppressWarnings("unchecked")
    private int compare(Tuple a, Tuple b) {
    	for (int i = 0; i < slots.length; i++) {
	        int comp = (a.getValue(slots[i])).compareTo(b.getValue(slots[i]));
	        if (comp != 0) return comp;
	    }
	    return 0;
    }
    
    /**
     * Provide an abstraction that treats a list of pages as a unified
     * scattered array, with the necessary operations on it for the
     * purposes of the enclosing class
     */
    private class PageList {

	    private String fileName;
	    private int numTuples, numTuplesPerPage, numPages, maxPages;
	    private Page[] pages;
	
	    /**
	     * Retrieve the tuple with the specified list index
	     */
	    private Tuple getTuple(int index) {
	        int pageIndex = index / numTuplesPerPage;
	        int tupleIndex = index % numTuplesPerPage;
	        return pages[pageIndex].retrieveTuple(tupleIndex);
	    }
	
	    /*
	     * Update the tuple of the specified list index, returning the
	     * previous value
	     */
	    private Tuple setTuple(int index, Tuple tuple) {
	        int pageIndex = index / numTuplesPerPage;
	        int tupleIndex = index % numTuplesPerPage;
	        Tuple oldTuple = pages[pageIndex].retrieveTuple(tupleIndex);
	        pages[pageIndex].setTuple(tupleIndex, tuple);
	        return oldTuple;
	    }
	
	    /**
	     * Compare two tuples of the list using the specified keys
	     */
	    private int compare(int a, int b) {
	        return ExternalMergeSort.this.compare(getTuple(a), getTuple(b));
	    }
	
	    /**
	     * Swap two tuples of the list
	     */
	    private void swap(int a, int b) {
	        setTuple(a, setTuple(b, getTuple(a)));
	    }
	
	    /**
	     * Preserve the properties of a binary heap, used for sorting
	     */
	    private void heapify(int heapSize, int pos) {
	        int left = (pos + 1) * 2 - 1, right = (pos + 1) * 2, largest = pos;
	        if (left < heapSize && compare(left, largest) > 0) {
	        	largest = left;
	        }
	        if (right < heapSize && compare(right, largest) > 0) {
	        	largest = right;
	        }
	        if (largest != pos) {
		        swap(pos, largest);
		        heapify(heapSize, largest);
	        }
	    }
	
	    /**
	     * Sort the contents of the list using the heapsort algorithm
	     */
	    public void sort() {
	        for (int i = numTuples / 2 - 1; i >= 0; i--)
	        	heapify(numTuples, i);
	        for (int i = numTuples - 1; i > 0; i--) {
		        swap(i, 0);
		        heapify(i, 0);
	        }
	    }
	
	    /**
	     * Check whether the list is completely empty
	     */
	    public boolean empty() {
	        return numPages == 0;
	    }
	
	    /**
	     * Check whether the list has room for one more tuple
	     */
	    public boolean hasRoom(Tuple t) {
	        return numPages < maxPages || pages[numPages-1].hasRoom(t);
	    }
	
	    /**
	     * Add a new tuple to the list
	     */
           public boolean addTuple(Tuple tuple)
 	       throws EngineException, StorageManagerException {
	        if (numPages == 0 || !pages[numPages-1].hasRoom(tuple)) {
	        	if (numPages == maxPages) return false;
	        	pages[numPages] =
		            new Page(getOutputRelation(),
		                 new PageIdentifier(fileName, numPages));
	        	sm.registerPage(pages[numPages++]);
	        }
	        pages[numPages-1].addTuple(tuple);
	        numTuples++;
	        // all pages except the last are assumed to have the same
	        // number of tuples, as per
	        // <F789A1F8-2DB9-11D9-B10D-000A95E51B2C@inf.ed.ac.uk>
	        if (numPages == 1) numTuplesPerPage++;
	        return true;
	    }
	
	    /**
	     * Write out the pages of the list to disk
	     */
	    public void flush() throws StorageManagerException {
	    	try {
		        for (int i = 0; i < numPages; i++) {
		        	String fn = pages[i].getPageIdentifier().getFileName();
		        	DatabaseFile dbf = new DatabaseFile(fn, DatabaseFile.READ_WRITE);
		        	PageIOManager.writePage(dbf, pages[i]);
		        }
	    	}
	    	catch (FileNotFoundException sme) {
	    		StorageManagerException ee = new StorageManagerException("Could not flush sorted pages to disk.");
	            ee.setStackTrace(sme.getStackTrace());
	            throw ee;
	        }
	    }
	
	    public PageList(int maxPages, String fileName) {
	        this.fileName = fileName;
	        this.maxPages = maxPages;
	        numTuples = numTuplesPerPage = numPages = 0;
	        pages = new Page[maxPages];
	    }
    }

    /**
     * Merge some runs
     */
    private void merge(RelationIOManager[] inMan, RelationIOManager outMan)
    throws StorageManagerException, IOException {
	    Tuple[] tuples = new Tuple[inMan.length];
	    ArrayList<Iterator<Tuple>> iterators = new ArrayList<Iterator<Tuple>>();
	    
	    for (int i = 0; i < inMan.length; i++) {
	    	iterators.add(inMan[i].tuples().iterator());
	        if (iterators.get(i).hasNext()) {
	        	tuples[i] = iterators.get(i).next();
	        }
	        else {
	        	tuples[i] = null;
	        }
	    }
	    
	    for (;;) {
	        Tuple best = null;
	        int bestRun = 0;
	        for (int i = 0; i < inMan.length; i++) {
		        if (tuples[i] != null && (best == null || compare(best, tuples[i]) > 0)) {
		            best = tuples[bestRun = i];
		        }
	        }
	        if (best == null) break;
	        outMan.insertTuple(best);
            if (iterators.get(bestRun).hasNext()) {
	        	tuples[bestRun] = iterators.get(bestRun).next();
	        }
	        else {
	        	tuples[bestRun] = null;
	        }
	    }
    }

    /**
     * Construct a new external sort operator
     * 
     * @param operator the input operator
     * @param sm the storage manager 
     * @param slots the slots acting as sort keys
     * @param buffers the number of buffers (i.e.,
     * output files) to be used for the sort
     * @throws EngineException thrown whenever the sort operator
     * cannot be properly initialized
     */
    public ExternalMergeSort (Operator operator, 
                              StorageManager sm,
                              int [] slots, 
                              int buffers) 
    throws EngineException {
        super(operator);
        this.sm = sm;
        this.slots = slots;
        this.buffers = buffers;
    	/*
        for (int i = 0 ; i < slots.length; i++)
    	    System.out.print(slots[i]);
    	System.out.println();*/
    } // ExternalSort()
    
    /**
     * Construct a new sort operator given an array of
     * input operators
     * 
     * @param inOps the array of input operators
     * @param sm the storage manager 
     * @param slots the slots acting as sort keys
     * @param buffers the number of buffers (i.e.,
     * output files) to be used for the sort
     * @throws EngineException thrown whenever the sort operator
     * cannot be properly initialized
     */
    public ExternalMergeSort (Operator [] inOps, 
                             StorageManager sm,
                             int [] slots,
                             int buffers)
    throws EngineException {
        this(inOps[0], sm, slots, buffers);
    } // ExternalSort()
    
    /**
     * Set up this external sort operator
     * 
     * @throws EngineException thrown whenever there is something wrong with
     * setting this operator up
     * @throws IOException when retrieving a tuple
     */
    public void setup () throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // this is a blocking operator -- store the input
            // in a temporary file and sort the file
            //
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // YOUR CODE GOES HERE
            //
            ////////////////////////////////////////////

	        // produce the sorted runs
	        ArrayList<String> tempFiles = new ArrayList<String>();
	        ArrayList<RelationIOManager> tempManagers = new ArrayList<RelationIOManager>();
	        
	        Tuple tuple = getInputOperator().getNext();
	        for (boolean more = true; more; ) {
	
		        // create a new temporary file for the run
		        String tempFile = FileUtil.createTempFileName();
		        sm.createFile(tempFile);
		        tempFiles.add(tempFile);
		        RelationIOManager tempMan =
		            new RelationIOManager(sm, getOutputRelation(), tempFile);
		        tempManagers.add(tempMan);
		
		        // paginate as many input tuples as we have memory for
		        PageList pageList = new PageList(buffers, tempFile);
		        while (pageList.hasRoom(tuple)) {
		            if (tuple != null) {
			            if (tuple instanceof EndOfStreamTuple) {
			                more = false;
			                break;
			            }
			            // the size of the array returned by
			            // getNext() is assumed to be 1, as per
			            // <A992D336-2E8B-11D9-B10D-000A95E51B2C@inf.ed.ac.uk>
			            
			            pageList.addTuple(tuple);
			            
		            }
		            tuple = getInputOperator().getNext();
		        }
		        if (pageList.empty()) break;
		
		        // sort the tuples, write them out, that's it!
		        
		        pageList.sort();
		        pageList.flush();
		        
	        }
	        
	        // merge the runs, until only one of them is left
	        while (tempFiles.size() > 1) {
		        ArrayList<String> nextTempFiles = new ArrayList<String>();
		        ArrayList<RelationIOManager> nextTempManagers = new ArrayList<RelationIOManager>();
		
		        // deal with at most buffers-1 runs at a time
		        for (int i = 0; i < tempManagers.size(); i += buffers - 1) {
		            int numRuns = tempManagers.size() - i;
		            if (numRuns > buffers - 1) numRuns = buffers - 1;
		
		            // pick out the runs that will be merged
		            RelationIOManager[] currRuns = new RelationIOManager[numRuns];
		            for (int j = 0; j < numRuns; j++) {
		            	currRuns[j] = (RelationIOManager) tempManagers.get(i+j);
		            }
		            
		            // create a new temporary file for the result
		            String tempFile = FileUtil.createTempFileName();
		            sm.createFile(tempFile);
		            nextTempFiles.add(tempFile);
		            RelationIOManager tempMan = new RelationIOManager(sm, getOutputRelation(), tempFile);
		            nextTempManagers.add(tempMan);
		            
		            // merge
		            merge(currRuns, tempMan);
		        }
	
		        // clean up
		        for (int i = 0; i < tempFiles.size(); i++) {
		            sm.deleteFile((String) tempFiles.get(i));
		        }
		        tempFiles = nextTempFiles;
		        tempManagers = nextTempManagers;
	        }
	        

	
	        // the last temporary file is the output file
	        outputFile = (String) tempFiles.get(0);
	        outputMan = (RelationIOManager) tempManagers.get(0);
	        outputTuples = outputMan.tuples().iterator();
	        
	        returnList = new ArrayList<Tuple>();

            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////
                
//            outputMan.openTupleCursor();
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not store intermediate relations to files.");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
        catch (IOException ioe) {
            IOException ee = new IOException("Error while retrieving a tuple.");
            ee.setStackTrace(ioe.getStackTrace());
        }
    } // setup()
    
    /**
     * Cleanup after the sort
     * 
     * @throws EngineException whenever the operator cannot clean up after
     * itself
     */
    public void cleanup () throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete the intermediate
            // files after sorting is done
            //
            ////////////////////////////////////////////

            ////////////////////////////////////////////
            //
            // right now, only the output file is 
            // deleted
            //
            ////////////////////////////////////////////
            sm.deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not clean up " +
                                                     "final output");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // cleanup()

    /**
     * The inner method to retrieve tuples
     * 
     * @return the newly retrieved tuples
     * @throws EngineException thrown whenever the next iteration is not 
     * possible
     */
    protected List<Tuple> innerGetNext() throws EngineException {
    	try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples " +
                                      "from intermediate file.", sme);
        }
    } // innerGetNext()

    /**
     * Operator class abstract interface -- never called
     */
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
    throws EngineException {
        return null;
    } // innerProcessTuple()

    /**
     * Operator class abstract interface -- sets the ouput relation of
     * this sort operator
     * 
     * @return this operator's output relation
     * @throws EngineException whenever the output relation of this operator
     * cannot be set
     */
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // setOutputRelation()

} // ExternalSort