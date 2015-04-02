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

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;

import org.dejave.attica.model.Relation;
import org.dejave.attica.engine.predicates.Predicate;
import org.dejave.attica.engine.predicates.PredicateEvaluator;
import org.dejave.attica.engine.predicates.PredicateTupleInserter;
import org.dejave.attica.storage.IntermediateTupleIdentifier;
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
public class MergeJoin extends PhysicalJoin {
	
    /** The name of the temporary file for the output. */
    private String outputFile;
    
    /** The list to hold temporary file names for cleanning up */
    private List<String> tempFileList;
    
    /** The size of array to hold group in memory */
    private int groupMemory = 10;
    
    /** The list to hold tuple group from right partition*/
    private Tuple[] groupTuples;
    
    /** The relation manager used for I/O. */
    private RelationIOManager outputMan;
    
    /** The pointer to the left sort attribute. */
    private int leftSlot;
	
    /** The pointer to the right sort attribute. */
    private int rightSlot;

    /** The iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable output list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new mergejoin operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @param leftSlot pointer to the left sort attribute.
     * @param rightSlot pointer to the right sort attribute.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
    public MergeJoin(Operator left, 
                     Operator right,
                     StorageManager sm,
                     int leftSlot,
                     int rightSlot,
                     Predicate predicate) 
	throws EngineException {
        
        super(left, right, sm, predicate);
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        returnList = new ArrayList<Tuple>();
        tempFileList = new ArrayList<String>();
        try {
            // create output io manager
            initTempFiles();
            outputMan = new RelationIOManager(getStorageManager(), 
                                              getOutputRelation(), 
                                              outputFile);
            getStorageManager().createFile(outputFile);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not instantiate " +
                                                     "merge join");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // MergeJoin()


    /**
     * Initialise the temporary files -- if necessary.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        //
        ////////////////////////////////////////////
        outputFile = FileUtil.createTempFileName();
        tempFileList.add(outputFile);
    } // initTempFiles()

    
    /**
     * Sets up this grace hash join operator.
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
            
            // store the left input
            Relation leftRel = getInputOperator(LEFT).getOutputRelation();
            initTempFiles();
            getStorageManager().createFile(outputFile);
            RelationIOManager leftMan =
                new RelationIOManager(getStorageManager(), leftRel, outputFile);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator(LEFT).getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) leftMan.insertTuple(tuple);
                }
            }
            
            // store the right input
            Relation rightRel = getInputOperator(RIGHT).getOutputRelation();
            initTempFiles();
            getStorageManager().createFile(outputFile);
            RelationIOManager rightMan = 
                new RelationIOManager(getStorageManager(), rightRel, outputFile);
            done = false;
            while (! done) {
                Tuple tuple = getInputOperator(RIGHT).getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) rightMan.insertTuple(tuple);
                }
            }
            
            // perform the merge join
            
            // create temp file to store group
            String groupFile = FileUtil.createTempFileName();
            tempFileList.add(groupFile);
            
            // size of group to buffer
            int groupSize = 0;
            
            // initialize array to store group if it is small engough
            groupTuples = new Tuple[groupMemory];
            
            Iterator<Tuple>left = leftMan.tuples().iterator();
            Iterator<Tuple>right = rightMan.tuples().iterator();
            
            Tuple leftTuple = left.next(); // first tuple from left partition
            Tuple rightScan = right.next(); // first tuple from right partition
            
            Iterator<Tuple>rGroup = rightMan.tuples().iterator();
            
            Tuple rightTuple = rightScan;
            Tuple previousTuple = new Tuple();
            
            Boolean more = true; // flag to check if scan of right partition done
            
            RelationIOManager groupMan = 
                    new RelationIOManager(getStorageManager(),
                                          rightRel,
                                          groupFile);
            
            // check if scan on either partition finished
            while (more){
                                                
                while (compare(leftTuple, rightTuple) < 0) {
                    // advance left partition index and scan the next tuple
                    leftTuple = left.next();
                }
                
                while (compare(leftTuple, rightTuple) > 0) {
                    // advance right partition index and scan the next tuple
                    rightScan = right.next();
                    rightTuple = rightScan;
                }
                
                while (compare(leftTuple, rightTuple) == 0) {
                    
                    // if left tuple has different value at leftSlot than previous time,
                    // create new file to buffer new group
                    if (previousTuple.getTupleIdentifier() == null 
                            || compare(leftTuple, previousTuple) != 0) {
                        
                        // initialize buffer file on disk
                        getStorageManager().deleteFile(groupFile);
                        getStorageManager().createFile(groupFile);
                        
                        // initialize buffer array in memory
                        groupSize = 0;
                        groupTuples = new Tuple[groupMemory];
                        
                        // scan following tuples of right tuple and buffer group to memory or disk
                        while(compare(leftTuple, rightScan) == 0)
                        {
                            previousTuple = rightScan;
                            
                            // decide buffer it to array in memory or file on disk
                            if (groupSize < groupMemory){
                                // try to use array to buffer the group
                                groupTuples[groupSize++] = rightScan;
                            } else {
                                // if exceeds, buffer it to disk 
                                if (groupSize == groupMemory){
                                    // first move content in memory to disk, then write remainder to disk
                                    for (Tuple t: groupTuples) {
                                        groupMan.insertTuple(t);
                                    }
                                }
                                groupMan.insertTuple(rightScan);
                                groupSize ++;
                            }
                            
                            // scan the tuple after the one just stored
                            if (right.hasNext()) {
                                rightScan = right.next();
                            } else {
                                more = false;
                                break;
                            }
                        }
                    }
                    
                    // combine left tuple and each right tuple from the buffered group, output the new tuple
                    if (groupSize > groupMemory){
                        // reset pointer to the beginning of the group by resetting iterator over it
                        rGroup = groupMan.tuples().iterator();
                        // scan and join part
                        while (rGroup.hasNext()){
                            rightTuple = rGroup.next();
                            
                            // insert joint tuple
                            Tuple newTuple = combineTuples(leftTuple, rightTuple);
                            outputMan.insertTuple(newTuple);
                            
                        }
                    } else {
                        for (int i = 0; i < groupMemory && groupTuples[i] != null; i++) {
                            // insert joint tuple
                            Tuple newTuple = combineTuples(leftTuple, groupTuples[i]);
                            outputMan.insertTuple(newTuple);
                        }
                    }
                    
                    // advance left partition scan
                    if (left.hasNext()){
                        leftTuple = left.next();
                    } else {
                        more = false;
                        break;
                    }
                }
                
                rightTuple = rightScan;
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
     * Compare tuple from left partition and tuple from right partition
     * on join slot
     * @param leftTuple tuple from left input
     * @param rightTuple tuple from right input
     * @return negative value when left tuple less than right tuple, 
     * 0 when equal, and positive when greater
     */
    private int compare(Tuple leftTuple, Tuple rightTuple) {
        int result = leftTuple.getValue(leftSlot).compareTo(rightTuple.getValue(rightSlot));
        return result;
    }

} // MergeJoin
