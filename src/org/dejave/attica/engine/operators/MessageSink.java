/*
 * Created on Dec 24, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.List;
import java.util.ArrayList;

import org.dejave.attica.model.Relation;

import org.dejave.attica.storage.Tuple;
import org.dejave.attica.storage.TupleIdentifier;

/**
 * MessageSink: A simple interface to provide DB messages to the user
 * as an operator.
 *
 * @author sviglas
 */
public class MessageSink extends Sink {
	
    /** This sink's message. */
    private String message;

    /** Reusable return list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new message sink.
     * 
     * @param message the message.
     * @throws EngineException thrown whenever the operator cannot
     * be properly constructed.
     */
    public MessageSink(String message) throws EngineException {
        super();
        this.message = message;
        returnList = new ArrayList<Tuple>();
    } // MessageSink()

    
    /**
     * Overriden method to retrieve this sink's message.
     * 
     * @return the tuple list containing the message
     * @throws EngineException thrown whenever the message cannot be retrieved
     */
    @Override
    public List<Tuple> getMultiNext() throws EngineException {
        returnList.clear();
        TupleIdentifier tid = new TupleIdentifier("DBResult", 0);
        List<Comparable> v = new ArrayList<Comparable>();
        v.add(message);
        Tuple tuple = new Tuple(tid, v);
        returnList.add(tuple);
        returnList.add(new EndOfStreamTuple());
        return returnList;
    } // getNext()

    
    /**
     * The inner tuple processing method -- doesn't do anything.
     * 
     * @param tuple the tuple to be processed.
     * @param inOp the index of the input operator the tuple to be
     * processed belongs to.
     * @return the resulting tuples
     * @throws EngineException thrown whenever there is something
     * wrong with processing the tuple.
     */
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        return new ArrayList<Tuple>();
    } // innerProcessTuple()

    
    /**
     * Doesn't do anything -- returns null by default.
     * 
     * @return a new output relation.
     * @throws EngineException whenever an output relation cannot be
     * constructed.
     */
    protected Relation setOutputRelation() throws EngineException {
        return null;
    } // setOutputRelation()
    
} // MessageSink
