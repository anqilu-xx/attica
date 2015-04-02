/*
 * Created on Dec 12, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.algebra;

/**
 * VariableValueQualification: A qualification between a variable and
 * a value.
 *
 * @author sviglas
 */
public class VariableValueQualification extends Qualification {
	
    /** The variable of this qualification. */
    private Variable variable;
	
    /** The value of this qualification. */
    private String value;
	
    /**
     * Constructs a new qualification between a variable and a value.
     * 
     * @param relationship the relationship between the variable and
     * the value.
     * @param variable the variable of this qualification.
     * @param value the value of this qualification.
     */
    public VariableValueQualification(Relationship relationship,
                                      Variable variable,
                                      String value) {
        super(relationship);
        this.variable = variable;
        this.value = value;
    } // VariableValueQualification()

    
    /**
     * Returns this qualification's variable.
     * 
     * @return the variable of this qualification.
     */
    public Variable getVariable() {
        return variable;
    } // getVariable()

    
    /**
     * Return this qualification's value.
     * 
     * @return the value of this qualification.
     */
    public String getValue() {
        return value;
    } // getValue()
	
    /**
     * Textual representation.
     */
    @Override
    public String toString() {
        return variable.toString() + " " + super.toString() + " " + value;
    } // toString()

} // VariableValueQualification()
