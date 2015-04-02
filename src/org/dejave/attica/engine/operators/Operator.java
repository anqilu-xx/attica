/*
 * Created on Dec 8, 2003 by sviglas
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
import java.util.NoSuchElementException;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Tuple;

/**
 * Operator: The basic operator abstraction. It implements all the
 * plubming for subsequent concrete operators. It is more general than
 * it need be, in the sense that it handles multi-input operators
 * (when in reality most operators will be unary or binary) and is
 * capable of producing multiple outputs with a single call. This can
 * be confusing at times, but, when extending it, it makes it much
 * easier to cater more execution models than a standard push/pull one
 * (e.g., symmetric hash join can be easily implemented since there is
 * a way to return multiple outputs per getNext() call.)
 *
 * Luckily, all this is masked to the programmer. The operator itself
 * buffers all multiple possible outputs and returns a single tuple to
 * the caller through a getNext() call.
 *
 * @author sviglas
 */
public abstract class Operator {
	
    /** A counter of already produced tuples -- all subclasses have access
     * to this counter. */
    protected int tupleCounter;
    
    /** Index of exhausted inputs. */
    private boolean [] done;
	
    /** The incoming operators. */
    private List<Operator> inOps;
	
    /** The number of incoming operators. */
    private int numInOps;
	
    /** This operator's output relation. */
    private Relation relation;
	
    /** Is this the first call to getNext() or not? */
    private boolean firstGetNext;

    private List<Tuple> buffer;
    private int bufferIndex;
    private boolean fromBuffer;
		
    /**
     * Default constructor.
     * 
     * @throws EngineException thrown whenever the operator cannot be
     * initialised.
     */
    public Operator() throws EngineException {
        this(new ArrayList<Operator>());
    } // Operator()

    
    /**
     * Constructs a new operator given the input operators.
     * 
     * @param inOps the list of input operators.
     * @throws EngineException thrown whenever the operator cannot be
     * initialised.
     */
    public Operator(List<Operator> inOps) throws EngineException {
        tupleCounter = 0;
        //relation = setOutputRelation();
        relation = null;
        firstGetNext = true;
        fromBuffer = false;
        bufferIndex = 0;
        setInputs(inOps);
    } // Operator()

    
    /**
     * Returns the number of inputs of this operator.
     * 
     * @return this operator's number of inputs.
     */
    public int getNumberOfInputs() {
        return numInOps;
    } // getNumerOfInputs()

    
    /**
     * Inner method to set the inputs of the operator.
     * 
     * @param inOps the input operators.
     */
    protected final void setInputs(List<Operator> inOps) {
        this.inOps = inOps;
        this.numInOps = inOps.size();
        done = new boolean [numInOps];
        for (boolean d : done) d = false;
    } // setInputs()

    
    /**
     * Retrieve the specified input operator.
     * 
     * @param which the index of the operator that should be retrieved
     * @return the specified input operator
     */
    public Operator getInputOperator(int which) throws EngineException {
        try {
            return inOps.get(which);
        }
        catch (ArrayIndexOutOfBoundsException aibe) {
            throw new EngineException("Retrieving a non-existing "
                                      + "input operator.", aibe);
        }
    } // getInputOperator()

    
    /**
     * Returns this operator's output relation.
     * 
     * @return this operator's output relation.
     */
    public Relation getOutputRelation() throws EngineException {
        if (relation == null) {
            relation = setOutputRelation();
        }
        return relation;
    } // getOutputRelation()

    
    /**
     * Some operators might need to perform some initial setting up.
     * This method is a placeholder for those operators.  By default
     * it does nothing.
     * 
     * @throws EngineException thrown whenever there is something wrong
     * during setup.
     */
    protected void setup() throws EngineException {
        // default implementation is a no-op
    } // setup()


    /**
     * Wrapper for the multi-output get next method.  In all
     * likelyhood, this is the one that will be called throughout,
     * unless the model is to be substantially extended.
     *
     * @return the next output tuple of this operator.
     * @throws EngineException whenever the next tuple cannot be
     * retrieved.
     */
    public Tuple getNext() throws EngineException {
        // first check if we're not reading from a buffer
        if (!fromBuffer) {
            // retrieve a buffer
            buffer = getMultiNext();
            // if there's nothing there, bail out; otherwise, start
            // scanning the buffer
            if (buffer == null || buffer.size() == 0) return null;
            else bufferIndex = 0;
        }

        // definitely in a buffer, so get the current tuple and set up
        // the next call
        Tuple t = buffer.get(bufferIndex++);
        fromBuffer = bufferIndex < buffer.size();
        return t;
    } // getNext()

    
    /**
     * Fetch the next tuple(s) from this operator.
     * 
     * @return the next tuples for this operator.
     * @throws EngineException thrown whenever there is something
     * wrong with retrieving tuples from this operator.
     */	
    public List<Tuple> getMultiNext() throws EngineException {
        // call setup the first time the operator is called -- NB: we
        // could not have called that during construction, because all
        // resources might not have been available then
        if (firstGetNext) {
            setup();
            firstGetNext = false;
        }

        // keep looping till we have a non-empty result
        List<Tuple> t = innerGetNext();
        while (t.size() == 0) t = innerGetNext();
        // clean up if we've seen the end
        if (t.size() == 1 && t.get(0) instanceof EndOfStreamTuple) cleanup();
        
        return t;
    } // getNext()
    
	
    /**
     * Called to clean up any resources held by the operator.  Default
     * implementation does nothing.
     * 
     * @throws EngineException thrown whenever there is something wrong 
     * during cleanup.
     */
    protected void cleanup() throws EngineException {
        // default implementation is a no-op
    } // cleanup()

    
    /**
     * Inner method to fetch the next tuple(s) from this operator.
     * The data flow of the standard implementation is to loop over
     * all non-exhausted inputs, retrieve tuples, process them, and put
     * them in an output list, which will be returned to the caller.
     * 
     * @return the next tuples for this operator.
     * @throws EngineException thrown whenever there is something
     * wrong with retrieving tuples from this operator.
     */
    protected List<Tuple> innerGetNext() throws EngineException {
        List<Tuple> outgoing = new ArrayList<Tuple>();

        int i = 0;
        for (Operator operator : inOps) {
            if (! done[i]) {
                List<Tuple> inTuples = operator.getMultiNext();
                for (Tuple tuple : inTuples) {
                    List<Tuple> temp = processTuple(tuple, i);
                    outgoing.addAll(temp);
                }
            }
            i++;
        }
        return outgoing;
    } // getNext()
	

    /**
     * Processes a single tuple from an operator, producing multiple
     * tuples in the output.
     * 
     * @param tuple the tuple to be processed.
     * @param inStream the index of the input operator this tuple
     * belongs to.
     * @return a list of output tuples.
     * @throws EngineException whenever there is an error while
     * processing the tuple.
     */
    protected List<Tuple> processTuple(Tuple tuple, int inStream) 
	throws EngineException {
        
        // first check whether this is an end of stream tuple
        if (tuple instanceof EndOfStreamTuple) {
            done[inStream] = true;
            List<Tuple> tuples = new ArrayList<Tuple>();
            if (allDone()) tuples.add(new EndOfStreamTuple());
            return tuples;
        }
        else {
            return innerProcessTuple(tuple, inStream);
        }
    } // processTuple()

    
    /**
     * Are all the incoming operators done or not?
     * 
     * @return <code>true</code> if all incoming operators are done, 
     * <code>false</code> otherwise.
     */
    protected boolean allDone() {
        for (boolean d : done) if (! d) return false;
        return true;
    } // allDone()

    
    /**
     * The inner tuple processing method -- all operators should implement 
     * this.
     * 
     * @param tuple the tuple to be processed.
     * @param inOp the index of the input operator the tuple to be
     * processed belongs to.
     * @return the resulting tuples.
     * @throws EngineException thrown whenever there is something wrong
     * with processing the tuple.
     */
    protected abstract List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException;
	
    /**
     * Subclasses should implement this method in order to specify the
     * output relation of the operator.
     * 
     * @return a new output relation.
     * @throws EngineException whenever an output relation cannot be 
     * constructed.
     */
    protected abstract Relation setOutputRelation() throws EngineException;
	
    /**
     * Textual representation.
     *
     * @return the textual representation of this operator.
     */
    @Override
    public String toString () {
        return toString(0);
    } // toString()

    
    /**
     * Convert only this operator to string and not the ones below.
     * 
     * @return the textual representation of this single operator.
     */
    protected String toStringSingle() {
        return "operator";
    } // toStringSingle()

    
    /**
     * Textual representation starting at a given level of the evaluation
     * plan.
     * 
     * @param level the level of the operator.
     * @return the textual representation of the operator.
     */
    public String toString(int level) {
        String prefix = prefix(level);
        StringBuffer sb = new StringBuffer();
        sb.append(prefix);
        sb.append(toStringSingle() + "\n");
        for (Operator operator : inOps)
            sb.append(operator.toString(level+1) + "\n");
        sb.setLength(sb.length()-1);
        return sb.toString();
    } // toString()

    
    /**
     * Create a prefix string for the textual representation.
     * 
     * @param level the level in the tree.
     * @return the prefix.
     */
    protected String prefix(int level) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < level; i++) sb.append("\t");
        return sb.toString();
    } // prefix()


    /**
     * Returns an iterable over the tuples returned by this operator.
     *
     * @return an iterable over the tuples of this operator.
     * @throws EngineException if the iterable cannot be constructed.
     */
    public Iterable<Tuple> tuples() throws EngineException {
        return new TupleIterable(this);
    }


    /**
     * A wrapper for iterable syntax over operators. Why? Because I
     * can.
     */
    class TupleIterable implements Iterable<Tuple> {
        /** Are there more elements in this iterator? */
        private boolean more;
        /** The next tuple to be returned. */        
        private Tuple tuple;

        private Operator operator;

        /**
         * Constructs a new iterable wrapper.
         *
         * @throws EngineException if the wrapper cannot be
         * constructed.
         */
        public TupleIterable(Operator op) throws EngineException {
            operator = op;
            tuple = operator.getNext();
            more = (tuple != null
                    ? (!(tuple instanceof EndOfStreamTuple))
                    : true);
        }

        /**
         * Returns the iterator implementation over the operator.
         *
         * @return the iterator over the tuples returned by this
         * operator.
         */
        public Iterator<Tuple> iterator() {
            return new Iterator<Tuple>(){
                /**
                 * Checks whether there are more tuples.
                 * @return <code>true</code> if there are more tuples,
                 * <code>false</code> otherwise.                 
                 */
                public boolean hasNext() {
                    return more;
                } // hasNext()
                /**
                 * Retrieves the next tuple from this iterator.
                 * @return the next tuple.
                 * @throws NoSuchElementException if there is
                 * something wrong when retrieving the tuple (most
                 * likely because we're requesting a tuple from an
                 * exhausted iterator).
                 */
                public Tuple next() throws NoSuchElementException {
                    try {
                        Tuple ret = tuple;
                        tuple = operator.getNext();
                        more = (tuple != null
                                ? (!(tuple instanceof EndOfStreamTuple))
                                : true);
                        return ret;
                    }
                    catch (EngineException ee) {
                        throw new NoSuchElementException("Cursor advance "
                                                         + "failed: "
                                                         + ee.getMessage());
                    }
                } // next()
                /**
                 * Removes the current tuple (unsupported -- will
                 * throw an exception by default).
                 * @throws UnsupportedOperationException by default --
                 * this is not supported by operators.
                 */
                public void remove() throws UnsupportedOperationException {
                    throw new UnsupportedOperationException("Cannot remove "
                                                            + "through "
                                                            + "an operator's "
                                                            + "iterator.");
                } // remove()
            }; // return new Iterator<Tuple>()
        } // iterator()
    } // TupleIterable

} // Operator
