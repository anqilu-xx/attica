/*
 * Created on Dec 20, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Tuple;

import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;

/**
 * Sink: An operator acting as a sink for other operators (i.e., it
 * simply saves its input and propagates it on getNext() calls).
 *
 * @author sviglas
 */
public class Sink extends UnaryOperator {
    
    /** The storage manager for the sink. */
    private StorageManager sm;
	
    /** The temporary file to store the result. */
    private String filename;
    
    /** The manager that undertakes relation I/O. */
    private RelationIOManager man;

    /** The iterator over tuples of the output file. */
    private Iterator<Tuple> tuples;

    /** The reusable list of return tuples. */
    private List<Tuple> returnList;
    
    /**
     * Default constructor.
     *
     * @throws EngineException whenever the operator cannot be
     * constructed.
     */
    public Sink() throws EngineException {
        super();
    } // Sink()

    
    /**
     * Constructs a new sink operator.
     * 
     * @param operator the input operator to this sink.
     * @param sm this sink's storage manager.
     * @param filename the name of the file where the data will be
     * stored
     * @throws EngineException thrown whenever the operator cannot be
     * properly initialised.
     */
    public Sink(Operator operator, StorageManager sm, String filename) 
	throws EngineException {
        
        super(operator);
        this.sm = sm;
        this.filename = filename;
    } // Sink()

    
    /**
     * Sets up this sink operator.
     * 
     * @throws EngineException whenever the sink cannot be set up.
     */
    @Override
    protected void setup() throws EngineException {
        try {
            sm.createFile(filename);
            Relation rel = getInputOperator().getOutputRelation();
            man = new RelationIOManager(sm, rel, filename);
            boolean done = false;
            while (! done) {
                Tuple tuple = getInputOperator().getNext();
                if (tuple != null) {
                    done = (tuple instanceof EndOfStreamTuple);
                    if (! done) man.insertTuple(tuple);
                }
            }
            //man = new RelationIOManager(sm, rel, filename);
            // I should burn in hell for initialising fields after
            // construction, but I'll probably burn in hell anyway, so
            // what's one more reason...
            tuples = man.tuples().iterator();
            returnList = new ArrayList<Tuple>();
        }
        catch (IOException ioe) {
            throw new EngineException("Could not create output iterator.", ioe);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not store final output.", sme);
        } 
    } // setup()

    
    /**
     * Cleans up after all processing.
     * 
     * @throws EngineException thrown whenever the operator cannot
     * clean up.
     */
    @Override
    protected void cleanup() throws EngineException {
        try {
            sm.deleteFile(filename);
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not clean up final output", sme);
        }
    } // cleanup()
    

    /**
     * The inner method to retrieve tuples
     * 
     * @return the newly retrieved tuples.
     * @throws EngineException thrown whenever the next iteration is not 
     * possible.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (tuples.hasNext()) returnList.add(tuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()

    
    /**
     * Operator abstract interface -- never called.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        returnList.clear();
        return returnList;
    } // innerProcessTuple()

    
    /**
     * Operator abstract interface -- sets the ouput relation of
     * this sink operator.
     * 
     * @return this operator's output relation.
     * @throws EngineException whenever the output relation of this
     * operator cannot be set.
     */
    @Override
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // setOutputRelation()

    
    /**
     * Textual representation
     */
    @Override
    public String toStringSingle() {
        return "sink <" + filename + ">";
    } // toStringSingle()
    
} // Sink
