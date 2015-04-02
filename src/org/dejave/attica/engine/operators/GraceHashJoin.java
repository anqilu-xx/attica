/*
 * Created on Feb 11, 2004 by sviglas
 *
 * Modified on Feb 17, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;

import org.dejave.attica.model.Relation;
import org.dejave.attica.engine.predicates.Predicate;
import org.dejave.attica.engine.predicates.PredicateEvaluator;
import org.dejave.attica.engine.predicates.PredicateTupleInserter;
import org.dejave.attica.storage.IntermediateTupleIdentifier;
import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.FileUtil;

/**
 * MergeJoin: Implements a merge join. The assumptions are that the
 * input is already sorted on the join attributes and the join being
 * evaluated is an equi-join.
 *
 * @author sviglas
 * 
 */
public class GraceHashJoin extends PhysicalJoin {
	
    /** The name of the temporary file for the output. */
    private String outputFile;
    
    /** The name of the temporary file for the left (smaller) input. */
    private String leftFile;
    
    /** The name of the temporary file for the right (larger) input. */
    private String rightFile;
    
    /** The list to hold temporary file names for cleanning up */
    private List<String> tempFileList;
    
    /** The relation manager used for I/O. */
    private RelationIOManager outputMan;
    
    /** The pointer to the left sort attribute. */
    private int leftSlot;
	
    /** The pointer to the right sort attribute. */
    private int rightSlot;

    /** The number of buffers to be used for hash tables. */
    private int buffers;

    /** The iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable output list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new grace-hash join operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @param leftSlot pointer to the left sort attribute.
     * @param rightSlot pointer to the right sort attribute.
     * @param buffers the number of buffers to be used for the hash tables.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
    public GraceHashJoin(Operator left, 
			 Operator right,
			 StorageManager sm,
			 int leftSlot,
			 int rightSlot,
			 int buffers,
			 Predicate predicate) 
	throws EngineException {
	
        super(left, right, sm, predicate);
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        this.buffers = buffers;
        returnList = new ArrayList<Tuple>();
        
        // initialize list to hold tmp file names
        tempFileList = new ArrayList<String>();
        try {
            leftFile = FileUtil.createTempFileName();
            sm.createFile(leftFile);
            tempFileList.add(leftFile);
            
            rightFile = FileUtil.createTempFileName();
            sm.createFile(rightFile);
            tempFileList.add(rightFile);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not instantiate " +
                                                     "merge join");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // GraceHashJoin()
    
    /**
     * Sets up this merge join operator.
     * 
     * @throws EngineException thrown whenever there is something
     * wrong with setting this operator up.
     */
    
    @Override
    protected void setup() throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // YOUR CODE GOES HERE
            //
            ////////////////////////////////////////////
            
            // store left input
            Relation leftRel = getInputOperator(LEFT).getOutputRelation();
            RelationIOManager leftMan =
                new RelationIOManager(getStorageManager(), leftRel, leftFile);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator(LEFT).getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) leftMan.insertTuple(tuple);
                }
            }
            
            // store right input
            Relation rightRel = getInputOperator(RIGHT).getOutputRelation();
            RelationIOManager rightMan =
                new RelationIOManager(getStorageManager(), rightRel, rightFile);
            done = false;
            while (! done) {
                Tuple tuple = getInputOperator(RIGHT).getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) rightMan.insertTuple(tuple);
                }
            }
            
            // compute number of pages inside input file
            int pageNum = FileUtil.getNumberOfPages(leftFile);
            int partNum = pageNum > buffers ? (int)(2*pageNum/buffers + 0.5) : (int)(2*pageNum);
            
            // initialize left partition files
            List<RelationIOManager> leftPartMans = new ArrayList<RelationIOManager>();
            leftPartMans = initPartitionFiles(leftPartMans, leftRel, "left", partNum);
            
            // initialize right partition files
            List<RelationIOManager> rightPartMans = new ArrayList<RelationIOManager>();
            rightPartMans = initPartitionFiles(rightPartMans, rightRel, "right", partNum);
            
            // partition left input
            Iterator<Tuple> leftTuples = leftMan.tuples().iterator();
            while(leftTuples.hasNext()){
                Tuple tuple = leftTuples.next();
                partition(tuple, leftPartMans, leftSlot);
            }
            
            // partition right input
            Iterator<Tuple> rightTuples = rightMan.tuples().iterator();
            while(rightTuples.hasNext()){
                Tuple tuple = rightTuples.next();
                partition(tuple, rightPartMans, rightSlot);
            }
            
            // create output file
            outputFile = FileUtil.createTempFileName();
            getStorageManager().createFile(outputFile);
            outputMan = new RelationIOManager(getStorageManager(), 
                                              getOutputRelation(),
                                              outputFile);
            tempFileList.add(outputFile);
            
            // handle each left partition, and the corresponding right partition
            for (int i = 0; i < partNum; i++){
                
                // build in-memory hash table for tuples in current left partition
                Hashtable<Integer, List<Tuple>> hashTable = new Hashtable<Integer, List<Tuple>>();
                RelationIOManager leftPartMan = leftPartMans.get(i);
                Iterator<Tuple> leftPartTuples = leftPartMan.tuples().iterator();
                while(leftPartTuples.hasNext()) {
                    Tuple tuple = leftPartTuples.next();
                    
                    // compute hash code of value in join slot of current tuple
                    int hashCode = tuple.getValue(leftSlot).hashCode();
                    
                    // get existing tuple list from hash table if hash code exists
                    // otherwise, create a tuple list
                    List<Tuple> tupleGroup = new ArrayList<Tuple>();
                    if(hashTable.containsKey(hashCode)){
                        tupleGroup = hashTable.get(hashCode);
                    }
                    
                    // insert current tuple into list
                    tupleGroup.add(tuple);
                    
                    // store it into hash table with hash code of join slot as key
                    hashTable.put(hashCode, tupleGroup);
                }
                
                // scan tuples in current right partition
                RelationIOManager rightPartMan = rightPartMans.get(i);
                Iterator<Tuple> rightPartTuples = rightPartMan.tuples().iterator();
                while(rightPartTuples.hasNext()) {
                    Tuple rightTuple = rightPartTuples.next();
                    int hashCode = rightTuple.getValue(rightSlot).hashCode();
                    
                    // look hash code of right join slot up in hash table
                    if(hashTable.containsKey(hashCode)){
                        
                        // output all matched left tuples from the specific tuple group
                        List<Tuple> tupleGroup = hashTable.get(hashCode);
                        for (Tuple leftTuple : tupleGroup) {
                            // insert joint tuple
                            Tuple newTuple = combineTuples(leftTuple, rightTuple);
                            outputMan.insertTuple(newTuple);
                        }
                    }
                }
            }
            
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////

            // open the iterator over the output
            outputTuples = outputMan.tuples().iterator();
        }
        catch (IOException ioe) {
            throw new EngineException("Could not create page/tuple iterators.",
                                      ioe);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not store " + 
                                                     "intermediate relations " +
                                                     "to files.");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // setup()
    
    
    /**
     * Cleans up after the join.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    @Override
    protected void cleanup() throws EngineException {
        try {
            for (String tmpFile : tempFileList) {
                getStorageManager().deleteFile(tmpFile);
            }
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not clean up " +
                                                     "final output");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // cleanup()

    /**
     * Inner method to propagate a tuple.
     * 
     * @return an array of resulting tuples.
     * @throws EngineException thrown whenever there is an error in
     * execution.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Inner tuple processing.  Returns an empty list but if all goes
     * well it should never be called.  It's only there for safety in
     * case things really go badly wrong and I've messed things up in
     * the rewrite.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        return new ArrayList<Tuple>();
    }  // innerProcessTuple()

    
    /**
     * Textual representation
     */
    protected String toStringSingle () {
        return "mj <" + getPredicate() + ">";
    } // toStringSingle()
    
    /**
     * Helper method to generate a partition file name.
     * @param prefix the string suggests path to temp files
     * @param input the string suggests "left" or "right" input
     * @param i the partition number for which it generated a file
     * name.
     * @return the partition file name corresponding to the given
     * partition number.
     */
    protected String generatePartitionFileName(String prefix, String input,int i) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append(".").append(input).append(".").append(i);
        return sb.toString();
    } // generatePartitionFileName()
    
    /**
     * Compute a suffix to distinguish different partition files
     * Decide which partition the tuple given belongs to by
     * computing a hash value on value in given join slot and 
     * take the result of modulo number of partitions
     * @param t tuple to decide which partition it belongs to
     * @param slot given join slot
     * @param divisor number of partitions
     * @return the suffix which leading to name of the file
     */
    private int getSuffix(Tuple t, int slot, int divisor) {     
        int partSuffix = t.getValue(slot).hashCode() % divisor;
        partSuffix = partSuffix < 0 ? partSuffix + divisor : partSuffix;
        return partSuffix;
    } // getSuffix
    
    /**
     * Create partition files with given partition number,
     * and bind them with RelationIOManager
     * @param partMans RelationIOManager list to hold managers 
     * of partition files
     * @param rel relation of input data to store in files
     * @param input "left" or "right" which will decide how the 
     * files will be named
     * @param partNum partition files number
     * @return list of created RelationIOManagers
     * @throws StorageManagerException
     */
    private List<RelationIOManager> initPartitionFiles(
                                        List<RelationIOManager> partMans, 
                                        Relation rel, 
                                        String input, 
                                        int partNum
                                        ) throws StorageManagerException {
        List<String> partFiles = new ArrayList<String>();
        String prefix = FileUtil.createTempFileName();
        
        for(int i = 0; i< partNum; i++){
            String partFile = generatePartitionFileName(prefix, input, i);
            partFiles.add(partFile);
            RelationIOManager partMan = 
                    new RelationIOManager(getStorageManager(), rel, partFile);
            partMans.add(partMan);
            getStorageManager().createFile(partFile);
            tempFileList.add(partFile);
            }
                            
        return partMans;
    } // initPartitionFiles
    
    /**
     * Decide which partition file given tuple belongs to and insert it in
     * @param tuple tuple to allocate to partitions
     * @param partMans list of RelationIOManagers of all available partition files
     * @param slot join slot of given tuple
     * @throws StorageManagerException
     */
    private void partition(Tuple tuple, 
            List<RelationIOManager> partMans, 
            int slot) 
                    throws StorageManagerException{
        int partNum = partMans.size();
        int partSuffix = getSuffix(tuple, slot, partNum);
        partMans.get(partSuffix).insertTuple(tuple);
    } // partition

} // MergeJoin
