/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Tuple;

import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.PageIdentifier;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;

import org.dejave.attica.storage.FileUtil;

/**
 * ExternalSort: Your implementation of sorting.
 *
 * @author sviglas
 */
public class ExternalSort extends UnaryOperator {
    
    /** The storage manager for this operator. */
    private StorageManager sm;
    
    /** The name of the temporary file for the output. */
    private String outputFile;
	
    /** The manager that undertakes output relation I/O. */
    private RelationIOManager outputMan;
	
    /** The slots that act as the sort keys. */
    private int [] slots;
	
    /** Number of buffers (i.e., buffer pool pages and 
     * output files). */
    private int buffers;

    /** Iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable tuple list for returns. */
    private List<Tuple> returnList;

    
    /**
     * Constructs a new external sort operator.
     * 
     * @param operator the input operator.
     * @param sm the storage manager.
     * @param slots the indexes of the sort keys.
     * @param buffers the number of buffers (i.e., run files) to be
     * used for the sort.
     * @throws EngineException thrown whenever the sort operator
     * cannot be properly initialized.
     */
    public ExternalSort(Operator operator, StorageManager sm,
                        int [] slots, int buffers) 
	throws EngineException {
        
        super(operator);
        this.sm = sm;
        this.slots = slots;
        this.buffers = buffers;
        try {
            // create the temporary output files
            initTempFiles();
            cleanup(outputFile);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not instantiate external sort",
                                      sme);
        }
    } // ExternalSort()
	

    /**
     * Initialises the temporary files, according to the number
     * of buffers.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        // in the event of an error
        //
        // for the time being, the only file we
        // know of is the output file
        //
        ////////////////////////////////////////////
        outputFile = FileUtil.createTempFileName();
        sm.createFile(outputFile);
    } // initTempFiles()

    
    /**
     * Sets up this external sort operator.
     * 
     * @throws EngineException thrown whenever there is something wrong with
     * setting this operator up
     */
    public void setup() throws EngineException {
        returnList = new ArrayList<Tuple>();
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
        	String inputFile = FileUtil.createTempFileName();
        	sm.createFile(inputFile);
        	
        	Relation rel = getInputOperator().getOutputRelation();
            RelationIOManager inputMan =
                new RelationIOManager(sm, rel, inputFile);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator().getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) inputMan.insertTuple(tuple);
                }
            }
                        
            @SuppressWarnings("unchecked")
            List<String> runNames = (List<String>) new ArrayList<String>();
            int B = buffers;
            
            //A Page Array containing B pages
            Page[] pages = new Page[B];
            int count = 0;
            
            //A List holding all tuples in B pages
            List<Tuple> tuplesInBPages = new ArrayList<Tuple>();
            
            //A List holding all tuples after sort in B pages
            List<Tuple> sortTuples = new ArrayList<Tuple>();
            
            //A Page Iterator over input files
            Iterator<Page> manPages = inputMan.pages().iterator();
            
            //Read in the input files in batches of B pages
            while (manPages.hasNext()){
            	Page p = manPages.next();
            	pages[count++] = p;
            	
            	//Undertake sorting when there come B pages or there comes the last page
            	if (count == B || (!manPages.hasNext())) {
            	    //Clear tuples list
            	    tuplesInBPages = new ArrayList<Tuple>();
            		//View the batches of B pages as a whole
            		for (Page e: pages){
            			if (e != null){
	            			Iterator<Tuple> it = e.iterator();
	            			while(it.hasNext()){
	            				tuplesInBPages.add(it.next());
	            			}
            			}
            		}
            		
            		//Sort each element in pages with quicksort
            		sortTuples = quicksort(tuplesInBPages);
            		
            		//Write the sort result to disk
            		initTempFiles();
            		RelationIOManager sortTupleMan =
            			new RelationIOManager(sm, rel, outputFile);

                    for (int i = 0; i < sortTuples.size(); i++)
                    {
                    	sortTupleMan.insertTuple(sortTuples.get(i));
                    }

            		runNames.add(outputFile);
            		
            		//Initialize count and wait for the next batches of B pages
            		count = 0;
            		
            		//Initialize pages
            		for (int i = 0; i < pages.length; i++){
            			pages[i] = null;
            		}
            	}
            	     	
            }

            //Merge B-1 files into one file
            while(runNames.size() > 1){
                //A list tracking output file names in new round
                List<String> newRunNames  = new ArrayList<String>();
            
                //An Iterator over temporary output file names
                Iterator<String> runNamesIt = runNames.iterator();
                
                //An Array holding names of B-1 files under process
                String[] currentNames = new String[B-1];
                
                count = 0;
                
                //Merge files if there are still files according to runNames
                while(runNamesIt.hasNext()) {
                	
                	String runName = runNamesIt.next();
                	currentNames[count++] = runName;
                	
                	if (count == B-1 || (!runNamesIt.hasNext())){
                	    //Initialize a output file when there come B-1 files
    		            initTempFiles();
    		            newRunNames.add(outputFile);
    		            
    		            //An Array holding index of current processing page of each file
    		            int[] pageIndexes = new int[B-1];
    		            
    		            //An Array holding index of current processing tuple of each page
    		            int[] tupleIndexes = new int[B-1];
    		            
    		            //A Page List holding B-1 pages from B-1 files
    		            Page[] srcPages = new Page[B-1];
    		            
    		            //A Page holding merged tuples from B-1 files
    		            Page outputPage = initPage(outputFile, sm, rel);
    		            
    		            //Initialize srcPages with first page of each file in processing
    		            for (int i = 0; i < B-1; i ++){
    		            	String mergeSrcFile = currentNames[i];
    		            	if(mergeSrcFile != null && !mergeSrcFile.isEmpty()){
        	            		srcPages[i] = sm.readPage(rel, new PageIdentifier(mergeSrcFile, 0));
    		            	} else {
    		            	    //If there are not B-1 files to merge,
    		            	    //set the page index and tuple index to -1
    		            	    pageIndexes[i] = -1;
    		            	    tupleIndexes[i] = -1;
    		            	}
    		            }
    		            
    		            //Index for the current processing file
    		            int fileCount = 0;
    		            
    		            //Index for the file having the smallest tuple so far
    		            int leastFileCount = 0;
    		            
    		            //Initialize the smallest tuple with the first tuple in the first page of the first file
    		            Tuple leastTuple = new Tuple();
    		            
    		            done = false;
    		            //Write the smallest tuple to a page until it is full then write it to disk and repeat until all B-1 files sorted to 1 file
    		            while(! done) {
    		                fileCount = 0;
    		                leastTuple = null;
    		                
    		            	for (Page srcPage: srcPages) {
    		            		int tupleInd = tupleIndexes[fileCount];
    		            		if (tupleInd != -1) {
    		            		    Tuple currentTuple = srcPage.retrieveTuple(tupleInd);
    		            		    
    		            		    //Initialize leastTuple as the first non empty tuple over B-1 files
                                    if (leastTuple == null) leastTuple = currentTuple;
        		            		if (compare(leastTuple, currentTuple) > 0) {
        		            			leastFileCount = fileCount;
        		            			leastTuple = currentTuple;
        		            		}
    		            		}
    		            		fileCount ++;
    		            	}
    		            	
    		            	if (!(outputPage.hasRoom(leastTuple))) {
    		            		//Write the full page to disk
    		            		sm.writePage(outputPage);
    		            		
    		            		//Add tuple to a new page
    		            		outputPage = initPage(outputFile, sm, rel);
    		            	}
    		            	
    		            	outputPage.addTuple(leastTuple);
    	            		tupleIndexes[leastFileCount] ++;
    	            		
    	            		//Check if there are still tuples in the page
    	            		if(tupleIndexes[leastFileCount] == srcPages[leastFileCount].getNumberOfTuples()) {
    	            			//If not, check if there are still pages in the file
    	            			if(pageIndexes[leastFileCount] == FileUtil.getNumberOfPages(currentNames[leastFileCount]) - 1) {
    	            				//If file done, set indexes of its pages and tuples to -1
    	            			    pageIndexes[leastFileCount] = -1;
    	            			    tupleIndexes[leastFileCount] = -1;
    	            			    srcPages[leastFileCount] = null;
    	            			    //Check if all files done
    	            			    done = true;
    	            			    for(int i = 0; i < B-1; i++){
    	            			        if (tupleIndexes[i] != -1){
    	            			            done = false;
    	            			            break;
    	            			        }
    	            			    }
    	            				
    	            			} else {
    	            				//If still pages, increment page index
    	            				pageIndexes[leastFileCount] ++;
    	            				//Swap processed page and newly registered page
    	            				Page nextOutPage = 
    	            				        sm.readPage(
    	            				                rel, 
    	            				                new PageIdentifier(currentNames[leastFileCount], pageIndexes[leastFileCount])
    	            				        );
    	            				srcPages[leastFileCount] = nextOutPage;
    	            				//Set the tuple index to 0
    	            				tupleIndexes[leastFileCount] = 0;
    	            			}
    	            		}
    		            }//Merge every B-1 files
    		            count = 0;
    		            for (int i = 0; i < B-1; i++){
    		                currentNames[i] = "";
                	    }
                	}
                }
                //Clear intermediate output files after use
                for(String tempFn : runNames){
                    cleanup(tempFn);
                }
                runNames = new ArrayList<String>(newRunNames);
            }

                        
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////

            outputMan = new RelationIOManager(sm, getOutputRelation(),
                                              outputFile);
            outputTuples = outputMan.tuples().iterator();
            
            //Clean the input file
            cleanup(inputFile);
            
        }
        catch (Exception sme) {
            throw new EngineException("Could not store and sort"
                                      + "intermediate files.", sme);
        }
    } // setup()

    
    /**
     * Cleanup after the sort.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    public void cleanup (String fileName) throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // make sure you delete the intermediate
            // files after sorting is done
            //
            ////////////////////////////////////////////
            
            sm.deleteFile(fileName);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not clean up final output.", sme);
        }
    } // cleanup()

    
    /**
     * The inner method to retrieve tuples.
     * 
     * @return the newly retrieved tuples.
     * @throws EngineException thrown whenever the next iteration is not 
     * possible.
     */    
    protected List<Tuple> innerGetNext () throws EngineException {
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
     * Operator class abstract interface -- never called.
     */
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        return new ArrayList<Tuple>();
    } // innerProcessTuple()

    
    /**
     * Operator class abstract interface -- sets the ouput relation of
     * this sort operator.
     * 
     * @return this operator's output relation.
     * @throws EngineException whenever the output relation of this
     * operator cannot be set.
     */
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // setOutputRelation()
    

    /**
     * Sort tuples with quick sort algorithm
     * @param input tuples to be sorted
     * @return sorted tuples list
     */
	private List<Tuple> quicksort(List<Tuple> input){
			
			if(input.size() <= 1){
				return input;
			}
			
			int middle = (int) Math.ceil((double)input.size() / 2);
			Tuple pivot = input.get(middle);
	
			List<Tuple> less = new ArrayList<Tuple>();
			List<Tuple> greater = new ArrayList<Tuple>();
			
			for (int i = 0; i < input.size(); i++) {
				if(i == middle){
					continue;
				}
				if(compare(input.get(i), pivot) < 0){
					less.add(input.get(i));
				}
				else{
					greater.add(input.get(i));
				}
			}
			
			return concatenate(quicksort(less), pivot, quicksort(greater));
	}
	
	/**
	 * Join the less tuples, pivot tuple and greater tuples
	 * to single list.
	 * @param less the less tuples
	 * @param pivot the tuple with mid index
	 * @param greater the greater tuples
	 * @return the tuples after join
	 */
	private List<Tuple> concatenate(List<Tuple> less, Tuple pivot, List<Tuple> greater){
			
			List<Tuple> list = new ArrayList<Tuple>();
			
			for (int i = 0; i < less.size(); i++) {
				list.add(less.get(i));
			}
			
			list.add(pivot);
			
			for (int i = 0; i < greater.size(); i++) {
				list.add(greater.get(i));
			}
			
			return list;
		}
	
	/**
	 * Compare given two tuples
	 * @param t1 one of the Tuples to be compared
	 * @param t2 the other of the Tuples to be compared
	 * @return 1 if t1 greater than or equals to t2, -1 if t1 less than t2
	 */
	private int compare(Tuple t1, Tuple t2) {
        int x;
        for (int i = 0; i < slots.length; i++) { // iterate over sorting slots
                x = t1.getValue(slots[i]).compareTo(t2.getValue(slots[i]));
                if (x != 0) return x; // return value if decision can be made
        }
        return 1; // or -1
}
	/**
	 * Register a page in output file given
	 * @param outputFile name of the file to which the page is registered
	 * @param sm storage manager instance
	 * @param rel relation of the page
	 * @return an registered empty page
	 * @throws StorageManagerException
	 */
	private Page initPage(String outputFile, StorageManager sm, Relation rel)
		throws StorageManagerException {
		try {
			int pageNum = FileUtil.getNumberOfPages(outputFile);
	        PageIdentifier pid = 
	            new PageIdentifier(outputFile, pageNum);
	        Page outputPage = sm.readPage(rel, pid);
	        return outputPage;
		} catch (Exception e) {
            e.printStackTrace(System.err);
            throw new StorageManagerException("I/O Error while initiate a output page when merging"
                                              + "to file: " + outputFile
                                              + " (" + e.getMessage() + ")", e);
        }
	}

} // ExternalSort
