/*
 * Created on Dec 13, 2003 by sviglas
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

import org.dejave.attica.model.Attribute;
import org.dejave.attica.model.Relation;

import org.dejave.attica.storage.BufferManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.Tuple;

/**
 * RelationScan: Scans an input relation from its primary file.
 *
 * @author sviglas
 */
public class RelationScan extends SourceOperator {
    /** The storage manager for this scan. */
    private StorageManager sm;
    
    /** The relation this scanner scans. */
    private Relation relation;
	
    /** The filename of the file storing the relation. */
    private String filename;
	
    /** The relation manager for this scan. */
    private RelationIOManager inputMan;

    /** The iterator over the input file. */
    private Iterator<Tuple> tuples;

    /** Reusable return list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new relation scan operator
     * 
     * @param sm this scanner's storage manager.
     * @param relation the relation this scanner scans.
     * @param filename the filename of the file that stores the
     * relation.
     * @throws EngineException thrown whenever the relation scanner
     * cannot be properly initialised.
     */
    public RelationScan(StorageManager sm, Relation relation, String filename) 
	throws EngineException {
        
        super();
        this.sm = sm;
        this.relation = relation;
        this.filename = filename;
        inputMan = new RelationIOManager(sm, relation, filename);
    } // RelationScan()

    
    /**
     * Fetch the filename that is to be scanned.
     * 
     * @return the filename this relation scanner scans.
     */
    public String getFileName() {
        return filename;
    } // getFileName()

    
    /**
     * Sets up the relation scan.
     * 
     * @throws EngineException whenever the relation scan cannot be
     * set up.
     */
    @Override
    protected void setup() throws EngineException {
        try {
            tuples = inputMan.tuples().iterator();
            returnList = new ArrayList<Tuple>();
        }
        catch (Exception sme) {
            throw new EngineException("Could not set up a relation scan.", sme);
        }
    } // setup()

    
    /**
     * Inner method to retrieve the next tuple(s).
     * 
     * @return an array of newly retrieved tuples.
     * @throws EngineException whenever the next tuple(s) cannot be
     * retrieved.
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
            throw new EngineException("Could not fetch a tuples from "
                                      + "a relation scan.", sme);
        }
    } // innerGetNext()


    /**
     * Inner processing of a tuple (never called, but made idempotent
     * for safety because I'm kinda stupid.)
     *
     * @param tuple the tuple to be processed.
     * @param inOp the source of this tuple.
     * @return empty list by default.
     * @throws EngineException never thrown by default (legacy call)
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        returnList.clear();
        return returnList;
    } // innerProcessTuple()
    

    /** 
     * Sets the output relation of this relation scan.
     * 
     * @return this relation scan's output relation.
     * @throws EngineException thrown whenever the output relation
     * cannot be set.
     */
    @Override
    protected Relation setOutputRelation() throws EngineException {
        return relation;
    } // setOutputRelation()

    
    /**
     * Textual representation.
     */
    protected String toStringSingle() {
        return "scan <" + filename + ">";
    } // toStringSingle()
	
    /**
     * Debug main
     * 
     * @param args arguments
     */
    public static void main (String [] args) {
        try {
            BufferManager bm = new BufferManager(100);
            StorageManager sm = new StorageManager(null, bm);
            
            List<Attribute> attributes = new ArrayList<Attribute>();
            attributes.add(new Attribute("integer", Integer.class));
            attributes.add(new Attribute("string", String.class));
            Relation relation = new Relation(attributes);
            String filename = args[0];
            
            RelationScan rs = new RelationScan(sm, relation, filename);
            boolean done = false;
            while (! done) {
                Tuple tuple = rs.getNext();
                System.out.println(tuple);
                done = (tuple instanceof EndOfStreamTuple);
            }
        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()
} // RelationScan
