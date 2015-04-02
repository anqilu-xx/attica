/*
 * Created on Dec 10, 2003 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.predicates;

import java.util.List;

/**
 * Conjunction: A conjunction of predicates.
 *
 * @author sviglas
 */
public class Conjunction extends ListPredicate implements Predicate {
	
    /**
     * Constructs a new conjunction.
     * 
     * @param list the list of predicates to be conjuncted.
     */
    public Conjunction(List<Predicate> list) {
        super(list);
    } // Conjunction()
	
    /**
     * Implements the <code>Predicate</code> interface by defining
     * the </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */
    public boolean evaluate() {
        
        for (Predicate predicate : predicates())
            if (! predicate.evaluate()) return false;
        return true;
    } // evaluate()

    
    /**
     * The symbol of the list.
     * 
     * @return this list's symbol.
     */
    @Override
    public String listSymbol() {
        return "AND";
    } // listSymbol()

} // Conjunction
