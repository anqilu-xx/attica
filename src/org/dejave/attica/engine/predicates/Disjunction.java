/*
 * Created on Dec 10, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 * 
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.predicates;

import java.util.List;

/**
 * Disjunction: A disjunction of predicates.
 *
 * @author sviglas
 */
public class Disjunction extends ListPredicate implements Predicate {
    
    /**
     * Constructs a new disjunction of predicates.
     * 
     * @param list the list of predicates to be disjuncted.
     */
    public Disjunction(List<Predicate> list) {
        super(list);
    } // Disjunction()

    
    /**
     * Implements the <code>Predicate</code> interface by defining
     * the </code>evaluate()</code> method.
     * 
     * @return <code>true</code> if the predicate evaluates to true,
     * <code>false</code> otherwise.
     */
    public boolean evaluate() {

        for (Predicate predicate : predicates())
            if (predicate.evaluate()) return true;
        return false;
    } // evaluate()

    
    /**
     * The symbol of the list.
     * 
     * @return this list's symbol.
     */
    @Override
    public String listSymbol() {
        return "OR";
    } // listSymbol()

} // Disjunction
