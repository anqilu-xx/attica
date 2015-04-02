/*
 * Created on Jan 12, 2015 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import javax.swing.text.StyledEditorKit.ForegroundAction;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.FileUtil;

/**
 * HashGroup: Your implementation of hash-based grouping.
 *
 * @author sviglas
 */
public class HashGroup extends UnaryOperator {
    
    /** The storage manager for this operator. */
    private StorageManager sm;
    
    /** The name of the temporary file for the output. */
    private String outputFile;
	
    /** The manager that undertakes output relation I/O. */
    private RelationIOManager outputMan;
	
    /** The slots that act as the group keys. */
    private int [] slots;
	
    /** Number of buffer pool pages to use. */
    private int buffers;

    /** Iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable tuple list for returns. */
    private List<Tuple> returnList;

    /** The list of partition files/managers. */
    private List<RelationIOManager> partitionFiles;

    /** The prefix name for all partition files. */
    private String partitionFilePrefix;
    
    /** The current partition being scanned for output. */
    private int currentPartition;
    
    /**
     * Constructs a new hash grouping operator.
     * 
     * @param operator the input operator.
     * @param sm the storage manager.
     * @param slots the indexes of the grouping keys.
     * @param buffers the number of buffers to be used for grouping.
     * @throws EngineException thrown whenever the grouping operator
     * cannot be properly initialized.
     */
    public HashGroup(Operator operator, StorageManager sm,
		     int [] slots, int buffers) 
	throws EngineException {
        
        super(operator);
        this.sm = sm;
        this.slots = slots;
        this.buffers = buffers;
        currentPartition = 0;
        partitionFiles = new ArrayList<RelationIOManager>();
        try {
            // create the temporary output files
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not instantiate external sort",
                                      sme);
        }
    } // HashGroup()
	

    /**
     * Initialises the temporary files, according to the number
     * of buffers.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
	try {
	    partitionFilePrefix = FileUtil.createTempFileName();
	    ////////////////////////////////////////////
	    //
	    // allocate any other files you see fit here
	    //
	    ////////////////////////////////////////////
	}
	catch (Exception e) {
	    e.printStackTrace(System.err);
	    throw new StorageManagerException("Could not instantiate temporary "
					      + "files for hash grouping", e);
	}
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
            // and then generate the partition files
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
            
            //Compute number of pages inside input file
            int pageNum = FileUtil.getNumberOfPages(inputFile);
            int partNum = pageNum > buffers ? (int)(2*pageNum/buffers + 0.5) : (int)(2*pageNum);
            
            //Initialize partition files
            initTempFiles();
            int count = 0;
            List<String> partFiles = new ArrayList<String>();
            
            for(int i = 0; i< partNum; i++){
                String partFile = generatePartitionFileName(i);
                partFiles.add(partFile);
                RelationIOManager partMan = 
                        new RelationIOManager(sm, rel, partFile);
                partitionFiles.add(partMan);
                sm.createFile(partFile);
                }
                        
            //A Page Iterator over input files
            Iterator<Page> manPages = inputMan.pages().iterator();
            
            //Decide which partition the tuple belongs to            
            while (manPages.hasNext()){
                Page p = manPages.next();
                
                //Process tuples in a page
                Iterator<Tuple> tuples = p.iterator();
                while(tuples.hasNext()){
                    Tuple t = tuples.next();
                    int partSuffix = getSuffix(t, partNum);
                    partitionFiles.get(partSuffix).insertTuple(t);                    
                }
            }
            
            
            ArrayList<ArrayList<String>> repartFiles = new ArrayList<ArrayList<String>>();
            ArrayList<ArrayList<RelationIOManager>> repartMans = 
                    new ArrayList<ArrayList<RelationIOManager>>();
            
            //Scan each partition file
            for (int i = 0; i < partNum; i++) {
                
                //Build hash table for regrouping
                ArrayList<String> currentReparts = new ArrayList<String>();
                ArrayList<RelationIOManager> currentRepartMans = new ArrayList<RelationIOManager>();
                Iterator<Page> pages= partitionFiles.get(i).pages().iterator();
                Hashtable<Integer, Integer> hashTable = new Hashtable<Integer, Integer>();
                while (pages.hasNext()){
                    Page p = pages.next();
                    Iterator<Tuple> tuples = p.iterator();
                    while(tuples.hasNext()){
                        Tuple t = tuples.next();
                        List<Comparable> slotVals = getSlotVal(t);        
                        int slotHash = combineHash(slotVals);
                        if (!hashTable.containsKey(slotHash))
                            hashTable.put(slotHash, hashTable.size());
                    }
                }
                
                //Initialize repartition files according to hash table
                int repartNum = hashTable.size();
                                
                if(repartNum > 0) {
                    
                    //Prefix for repartition file is file name of parent partition file
                    partitionFilePrefix = partFiles.get(i);
                    for (int j = 0; j < repartNum; j++) {
                        String repartFile = generatePartitionFileName(j);
                        currentReparts.add(repartFile);
                        RelationIOManager ioMan = 
                                new RelationIOManager(sm, rel, repartFile);
                        currentRepartMans.add(ioMan);
                        sm.createFile(repartFile);
                    }
                    
                    repartFiles.add(currentReparts);
                    repartMans.add(currentRepartMans);
                                
                    //Regroup
                    Iterator<Tuple> tuples = partitionFiles.get(i).tuples().iterator();
                    while(tuples.hasNext()) {
                        Tuple t = tuples.next();
                        List<Comparable> slotVals = getSlotVal(t);        
                        int slotHash = combineHash(slotVals);
                        int repartSuffix = hashTable.get(slotHash);
                        repartSuffix = repartSuffix < 0 ? repartSuffix + repartNum: repartSuffix;
                        //Insert tuple to new partition
                        currentRepartMans.get(repartSuffix).insertTuple(t);
                    }
                }
            }
            
            //Clean intermediate files
            for (String interFile : partFiles) {
                cleanup(interFile);
            }
            
            //Update partitionFiles
            partitionFiles = new ArrayList<RelationIOManager>();
            for (ArrayList<RelationIOManager> repartFilesMans: repartMans) {
                for(RelationIOManager m: repartFilesMans) {
                    partitionFiles.add(m);
                }
            }
                        
            ////////////////////////////////////////////
            //
            // the output should reside in multiple
            // output files; instantiate the first manager
            // to the first such file
            //
            ////////////////////////////////////////////
            currentPartition = 0;
            outputMan = partitionFiles.get(currentPartition);
            outputTuples = outputMan.tuples().iterator();
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
    public void cleanup (String filename) throws EngineException {
        try {
            sm.deleteFile(filename);
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
            else {
        		while (++currentPartition < partitionFiles.size()) {
        		    outputMan = partitionFiles.get(currentPartition);
        		    outputTuples = outputMan.tuples().iterator();
        		    while (outputTuples.hasNext()) {
        		        returnList.add(outputTuples.next());
        		    }
        		}
        		returnList.add(new EndOfStreamTuple());
            }
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
     * Helper method to generate a partition file name.
     *
     * @param i the partition number for which it generated a file
     * name.
     * @return the partition file name corresponding to the given
     * partition number.
     */
    protected String generatePartitionFileName(int i) {
	StringBuilder sb = new StringBuilder();
	sb.append(partitionFilePrefix).append(".").append(i);
	return sb.toString();
    } // generatePartitionFileName()
    
    /**
     * Compute a suffix to distinguish different partition files
     * Decide which partition the tuple given belongs to by
     * computing a hash value on values in each slot and take the
     * result of modulo number of partitions
     * @param t tuple to decide which partition it belongs to
     * @param divisor number of partitions
     * @return the suffix which leading to name of the file
     */
    private int getSuffix(Tuple t, int divisor) {
        List<Comparable> slotVals = getSlotVal(t);        
        int partSuffix = combineHash(slotVals) % divisor;
        partSuffix = partSuffix < 0 ? partSuffix + divisor : partSuffix;
        return partSuffix;
    }
    
    /**
     * Compute hash value over a combination of given values
     * @param vals values to compute hash value
     * @return hash value
     */
    private int combineHash(List<Comparable> vals) {
        int hash = 17;
        for (Comparable v : vals){
            hash = hash * 31 + v.hashCode();
        }
        return hash;
    }
    
    /**
     * Get values of the tuple given over each slot as 
     * a list of Comparables 
     * @param t tuple of which the values in slots need to compute
     * @return values in a list
     */
    private List<Comparable> getSlotVal(Tuple t) {
        List<Comparable> slotVals = new ArrayList<Comparable>();
        for (int i = 0; i < slots.length; i++) {
            slotVals.add(t.getValue(slots[i]));
        }
        return slotVals;
    }

} // HashGroup
