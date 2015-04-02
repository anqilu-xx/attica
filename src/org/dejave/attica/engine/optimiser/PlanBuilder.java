/*
 * Created on Dec 14, 2003 by sviglas
 *
 * Modified on Dec 26, 2008 by sviglas
 *
 * Modified on Jan 12, 2015 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.optimiser;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Map;
import java.util.HashMap;

import org.dejave.attica.engine.algebra.*;
import org.dejave.attica.engine.operators.*;
import org.dejave.attica.engine.predicates.*;
import org.dejave.attica.model.*;
import org.dejave.attica.storage.Catalog;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.server.Database;
import org.dejave.util.Triplet;


/**
 * PlanBuilder: Given a collection of algebraic operators, constructs
 * an evaluation plan to be used by the engine.
 *
 * @author sviglas
 */
public class PlanBuilder {
	
    /** The database catalog. */
    private Catalog catalog;

    /** The storage manager. */
    private StorageManager sm;

    /** The server parameters. */
    private Map<String, String> parameters;
	
    /**
     * Constructs a new plan builder instance, given a catalog and
     * a storage manager.
     * 
     * @param catalog the database catalog.
     * @param sm the storage manager.
     * @param parameters the server parameters.
     */
    public PlanBuilder(Catalog catalog, StorageManager sm,
		       Map<String, String> parameters) {
        this.catalog = catalog;
        this.sm = sm;
	this.parameters = parameters;
    } // PlanBuilder()

    
    /**
     * Given a collection of algebraic operators, construct the
     * evaluation plan and return its root.
     * 
     * @param operators the collection of algebraic operators.
     * @return the root of the evaluation plan.
     * @throws PlanBuilderException thrown whenever a plan cannot be
     * constructed.
     */
    public Operator buildPlan(String filename,
                              List<AlgebraicOperator> operators) 
	throws PlanBuilderException {

        //
        // The process is quite simple: first decompose the operators
        // into scans, projections, selections, and joins (i.e., the
        // conjunctive normal form of the query -- which is rather
        // fortunate since this is all we support anyway). Once these
        // are identified, perform the bare minimum. Drop all
        // unneccessary fields so we can reduce tuple width, then
        // impose all selections, enumerate joins, and finally impose
        // any projection lists and sort orders. Join enumeration is
        // syntactic, i.e., the order we apply the joins is the order
        // these joins are specified in the query. There are a few
        // quirks in the optimisation process, but nothing special.
        // Most of it is simple bookkeeping.
        //
        
        Operator operator = null;
	
        // get the relevant table names
        Set<String> tables = getTables(operators);
        // get the projections
        List<Projection> projections = getProjections(operators);
        // get the selections
        List<Selection> selections = getSelections(operators);
        // get the joins
        List<Join> joins = getJoins(operators);
	// get the sorts
        List<Sort> sorts = getSorts(operators);
	// and the groupings
	List<Group> groups = getGroups(operators);
	
        // now, start building the plan
	
        // first build the initial projections
        List<InitialProjection> iProjections =
            buildInitialProjections(projections, selections, joins, tables);
        
        // given the tables, build the relation scans
        List<RelationScan> scans = buildScans(tables);
        // impose the initial projections over the relation scans
        List<Operator> ips =
            imposeInitialProjections(iProjections, scans);
        // now, append the relevant selections over each scan
        List<Operator> planOps = imposeSelections(selections, ips);
        // then, order the joins and cartesians in a deep tree
        planOps = enumerateJoins(joins, planOps);
        // perform sanity check and impose the final projections
        if (planOps.size() != 1) {
            throw new PlanBuilderException("Multiple branches after join "
                                           + "enumeration.");
        }
        operator = planOps.get(0);
        operator = imposeFinalProjections(projections, operator);
        operator = imposeSorts(sorts, operator);
        operator = imposeGroups(groups, operator);
        try {
            operator = new Sink(operator, sm, filename);
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not build final sink "
                                           + "operator (" + ee.getMessage()
                                           + ").", ee);
        }
	
        // we're done baby, we're done...
        return operator;
    } // buildPlan()
	
    /**
     * Given a physical operator, return a set with the names
     * of all tables participating in it.
     * 
     * @param operator the physical operator
     * @return a set of the names of all tables participating in the
     * physical operator.
     * @throws PlanBuilderException whenever the tables cannot be
     * fetched.
     */
    protected Set<String> getTables(Operator operator) 
        throws PlanBuilderException {
        
        try {
            Set<String> tables = new LinkedHashSet<String>();
		
            Relation rel = operator.getOutputRelation();
            for (Attribute attribute : rel) {
                TableAttribute tab = (TableAttribute) attribute;
                String table = tab.getTable();
                tables.add(table);
            }
            
            return tables;
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", ee);
        }
    } // getTables()
	

    /**
     * Given a list of algebraic operators, return a set with the
     * names of all tables appearing in the operators.
     * 
     * @param operators a list of algebraic operators.
     * @return a set with the names of all tables appearing in the
     * algebraic operators.
     */
    protected Set<String> getTables(List<AlgebraicOperator> operators) {
        Set<String> inTables = new LinkedHashSet<String>();
        
        for (AlgebraicOperator alg : operators) {            
            if (alg instanceof Projection) {
                Projection p = (Projection) alg;
                for (Variable var : p.projections()) {
                    String table = var.getTable();
                    inTables.add(table);
                }
            }
            else if (alg instanceof Selection) {
                Selection s = (Selection) alg;
                VariableValueQualification vvq = 
                    (VariableValueQualification) s.getQualification();
                String table = vvq.getVariable().getTable();
                inTables.add(table);
            }
            else if (alg instanceof Join) {
                Join j = (Join) alg;
                VariableVariableQualification vvq =
                    (VariableVariableQualification) j.getQualification();
                String leftTable = vvq.getLeftVariable().getTable();
                String rightTable = vvq.getRightVariable().getTable();
                inTables.add(leftTable);
                inTables.add(rightTable);
            }
        }
	
        return inTables;
    } // getTables()

    
    /**
     * Given a list of projections, a list of selections and a list of
     * joins, return a vector of initial projections, i.e., the
     * projections that should be pushed down right above the scans.
     * 
     * @param projections a list of algebraic projections.
     * @param selections a list of algebraic selections.
     * @param joins a list of algebraic joins.
     * @param tables the set of tables names.
     * @return a list of new projection operators.
     */
    protected List<InitialProjection>
        buildInitialProjections(List<Projection> projections,
                                List<Selection> selections,
                                List<Join> joins,
                                Set<String> tables) {
        
        List<InitialProjection> initProjections =
            new ArrayList<InitialProjection>();
        
        for (String table : tables) {
            Set<Variable> plp = getAttributes(table, projections);
            Set<Variable> pls = getAttributes(table, selections);
            Set<Variable> plj = getAttributes(table, joins);
            plp.addAll(pls);
            plp.addAll(plj);
            // brain-damaged -- should be fixed
            InitialProjection ip =
                new InitialProjection(new ArrayList<Variable>(plp));
            initProjections.add(ip);
        }

        return initProjections;
    } // getInitialProjections()

    
    /**
     * Given a table name and a list of algebraic operators, 
     * return a projection list of all accessed attributes.
     * 
     * @param table the table name.
     * @param operators a list of algebraic operators.
     * @return a projection list of all accessed attributes.
     */
    protected Set<Variable>
        getAttributes(String table,
                      List<? extends AlgebraicOperator> operators) {

        Set<Variable> outList = new LinkedHashSet<Variable>();

        for (AlgebraicOperator alg : operators) {
            if (alg instanceof Projection) {
                Projection p = (Projection) alg;
                for (Variable var : p.projections()) {
                    String pTable = var.getTable();
                    if (table.equals(pTable)) outList.add(var);
                }
            }
            else if (alg instanceof Selection) {
                Selection s = (Selection) alg;
                VariableValueQualification vvq = 
                    (VariableValueQualification) s.getQualification();
                String sTable = vvq.getVariable().getTable();
                if (table.equals(sTable)) outList.add(vvq.getVariable());
            }
            else if (alg instanceof Join) {
                Join j = (Join) alg;
                VariableVariableQualification vvq =
                    (VariableVariableQualification) j.getQualification();
                String lTable = vvq.getLeftVariable().getTable();
                String rTable = vvq.getRightVariable().getTable();
                if (table.equals(lTable)) outList.add(vvq.getLeftVariable());
                if (table.equals(rTable)) outList.add(vvq.getRightVariable());
            }
        }
        
        return outList;
    } // getAttributes()

    
    /**
     * Given a set of tables, return a list of physical scan
     * operators.
     * 
     * @param tables a set of table names.
     * @return a list of table scan operators.
     * @throws PlanBuilderException thrown whenever the scans cannot
     * be constructed.
     */
    protected List<RelationScan> buildScans(Set<String> tables)
        throws PlanBuilderException {
        
        try {
            List<RelationScan> scans = new ArrayList<RelationScan>();

            for (String table : tables) {
                Table schema = catalog.getTable(table);
                String filename = catalog.getTableFileName(table);
                RelationScan rs = new RelationScan(sm, schema, filename);
                scans.add(rs);
            }
            
            return scans;
        }
        catch (NoSuchElementException nste) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", nste);
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not instantiate relation "
                                           + "scans.", ee);
        }
    } // buildScans()
	
    /**
     * Imposes the initial projections over the scan operations.
     * 
     * @param iProjections the vector of initial projections.
     * @param scans the vector of scan operators.
     * @return a new vector of physical operators, with the
     * projections on top of the scans.
     * @throws PlanBuilderException thrown whenever the projections cannot
     * be imposed.
     */
    protected List<Operator>
        imposeInitialProjections(List<InitialProjection> iProjections,
                                 List<RelationScan> scans) 
	throws PlanBuilderException {
        
        try {
            List<Operator> branches = new ArrayList<Operator>();

            for (RelationScan rs : scans) {
                InitialProjection ip = 
                    getRelevantInitialProjection(iProjections, rs);
                Operator op = rs;
                if (ip != null) {
                    Project p =
                        new Project(rs, convertProjectionList(
                            ip.projections(), rs.getOutputRelation()));
                    op = p;
                }
                branches.add(op);
            }
            
            return branches;
        }
        catch (EngineException ee) {
            PlanBuilderException pbe =
                new PlanBuilderException("Could not instantiate initial projections");
            pbe.setStackTrace(ee.getStackTrace());
            throw pbe;
        }		
    } // imposeInitialProjections()

    
    /**
     * Given a list of projections and a scan, return the relevant 
     * projection.
     * 
     * @param ips the list of initial projections.
     * @param rs the relation scan.
     * @return the relevant initial projection.
     * @throws StorageManagerException thrown whenever the initial
     * projections cannot be identified.
     */
    protected InitialProjection
        getRelevantInitialProjection(List<InitialProjection> ips,
                                     RelationScan rs) 
	throws PlanBuilderException {
        
        try {
            for (InitialProjection ip : ips) {
                // get the table of the projection
                String pTable = ip.getProjectionList().get(0).getTable();
                // get the table of the scan
                TableAttribute tab = 
                    (TableAttribute) rs.getOutputRelation().getAttribute(0);
                String sTable = tab.getTable();
                if (pTable.equals(sTable)) return ip;
            }
            return null;
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Initial projections could not "
                                           + "be imposed.", ee);
        }
    } // getRelevantInitialProjection()

    
    /**
     * Imposes the selections over execution branches.
     * 
     * @param selections the list of selections.
     * @param sources the incoming plan sources.
     * @return a list of new execution branches with the selections
     * imposed.
     * @throws PlanBuilderException thrown whenever the selections
     * cannot be imposed.
     */
    protected List<Operator> imposeSelections(List<Selection> selections,
                                              List<? extends Operator> sources) 
	throws PlanBuilderException {
        
        try {
            List<Operator> res = new ArrayList<Operator>();
            
            for (Operator op : sources) {
                TableAttribute tab = 
                    (TableAttribute) op.getOutputRelation().getAttribute(0);
                String oTable = tab.getTable();
                List<Selection> rSelections =
                    getRelevantSelections(selections, oTable);
                if (rSelections.size() != 0) {
                    Operator top = op;
                    for (Selection sIt : rSelections) {
                        Select ps = new Select(top,
                            convertQualification(sIt.getQualification(), 
                                                 top));
                        res.add(ps);
                        top = ps;
                    }
                }
                else {
                    res.add(op);
                }
            }
            
            return res;
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not instantiate selections.",
                                           ee);
        }
    } // imposeSelections()

    
    /**
     * Given a list of selections and a table name, identify the
     * relevant selections on that table.
     * 
     * @param selections the list of selections.
     * @param table a table name.
     * @return a list containing all the selections over the given
     * table.
     */
    protected List<Selection>
        getRelevantSelections(List<Selection> selections, String table) {
        
        List<Selection> v = new ArrayList<Selection>();
	
        for (Selection s : selections) {
            VariableValueQualification vvq =
                (VariableValueQualification) s.getQualification();
            String sTable = vvq.getVariable().getTable();
            if (table.equals(sTable)) v.add(s);
        }
	
        return v;
    } // getRelevantSelections()

    
    /**
     * Given an iterable collection of variables a relation, it
     * converts the collection into an array of slots.
     * 
     * @param pl the projection list.
     * @param rel the relation schema.
     * @return an array of projection slots.
     * @throws PlanBuilderException thrown whenever the conversion cannot
     * be performed.
     */
    protected int [] convertProjectionList(Iterable<Variable> pl,
                                           Relation rel) 
	throws PlanBuilderException {
        
        try {
            List<Integer> slots = new ArrayList<Integer>();
            
            for (Variable v : pl) {
                int i = 0;
                for (Attribute a : rel) {
                    TableAttribute tab = (TableAttribute) a;
                    if (v.getTable().equals(tab.getTable())
                        && v.getAttribute().equals(tab.getName()))
                        slots.add(i);
                    i++;
                }
                /*
                  this was a bit braindead, but I'll leave it here for
                  posterity...
                  
                boolean done = false;
                for (int j = 0; j < rel.getNumberOfAttributes() && !done; j++) {
                    TableAttribute tab = (TableAttribute) rel.getAttribute(j);
                    if (v.getTable().equals(tab.getTable()) &&
                        v.getAttribute().equals(tab.getName())) {
                        slots.add(new Integer(j));
                        done = true;
                    }
                }
                */
            }

            int [] ret = new int[slots.size()];
            int i = 0;
            for (Integer in : slots) ret[i++] = in.intValue();
            return ret;
        }
        catch (ClassCastException cce) {
            throw new PlanBuilderException("Could not cast attributes.", cce);
        }
    } // convertProjectionList()

    
    /**
     * Converts a logical qualification to a physical predicate.
     * 
     * @param q the qualification to be converted.
     * @param op the incoming physical operator.
     * @return the physical predicate.
     * @throws PlanBuilderException thrown whenever the phyiscal
     * predicate cannot be constructed.
     */
    protected Predicate convertQualification(Qualification q, Operator op) 
	throws PlanBuilderException {
        
        try {
            Relation rel = op.getOutputRelation();
            if (q instanceof VariableValueQualification) {
                VariableValueQualification vvq =
                    (VariableValueQualification) q;
                Variable var = vvq.getVariable();
                TupleSlotPointer tsp = createSlotPointer(var, rel);
                Comparable c = createComparable(tsp.getType(), 
                                                vvq.getValue());
                return new TupleValueCondition(tsp, c, 
                    translateRelationship(vvq.getRelationship()));
            }
            else if (q instanceof VariableVariableQualification) {
                VariableVariableQualification vvq =
                    (VariableVariableQualification) q;
                Variable leftVar = vvq.getLeftVariable();
                TupleSlotPointer leftTsp = createSlotPointer(leftVar, rel);
                Variable rightVar = vvq.getRightVariable();
                TupleSlotPointer rightTsp = createSlotPointer(rightVar, rel);
                return new TupleTupleCondition(leftTsp, rightTsp,
                    translateRelationship(vvq.getRelationship()));
            }
            else {
                return new TrueCondition();
            }
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", ee);
        }
    } // convertQualification()
    

    /**
     * Converts a qualification to a phyical predicate (used for joins).
     * 
     * @param q the qualification to be converted.
     * @param left the incoming left source.
     * @param right the incoming right source.
     * @return the resulting predicate.
     * @throws PlanBuilderException thrown whenever the predicate
     * cannot be constructed.
     */
    protected Predicate convertQualification(Qualification q, 
                                             Operator left,
                                             Operator right) 
	throws PlanBuilderException {
        
        try {
            Relation leftRel = left.getOutputRelation();
            Relation rightRel = right.getOutputRelation();
            if (q instanceof VariableVariableQualification) {
                VariableVariableQualification vvq = 
                    (VariableVariableQualification) q;
                Variable leftVar = vvq.getLeftVariable();
                TupleSlotPointer leftTsp = createSlotPointer(leftVar, leftRel);
                Variable rightVar = vvq.getRightVariable();
                TupleSlotPointer rightTsp = createSlotPointer(rightVar, rightRel);
                return new TupleTupleCondition(leftTsp, rightTsp,
                    translateRelationship(vvq.getRelationship()));
            }
            else {
                return new TrueCondition();
            }
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", ee);
        }
    } // convertQualification()
    
	
    /**
     * Given a variable and a relation, it creates a slot pointer.
     * 
     * @param var the variable to be converted.
     * @param r the incoming relation.
     * @return the slot pointer pointing to the variable in the
     * relation.
     */
    protected TupleSlotPointer createSlotPointer(Variable var, Relation r) {
        int i = 0;
        String vTable = var.getTable();
        String vAttr = var.getAttribute();
        for (Attribute attr : r) {
            TableAttribute tab = (TableAttribute) attr;
            String rTable = tab.getTable();
            String rAttr = tab.getName();
            if (vTable.equals(rTable) && vAttr.equals(rAttr)) {
                return new TupleSlotPointer(tab.getType(), i);
            }
            i++;
        }
	
        return null;
    } // createSlotPointer()
    
	
    /**
     * Given a type and a value, it creates a comparable object for
     * use in the physical predicates.
     * 
     * @param type the type.
     * @param value the value of the logical qualification.
     * @return a comparable object to be used in a physical predicate.
     * @throws PlanBuilderException thrown whenever a comparable object
     * cannot be instantiated.
     */
    protected Comparable createComparable(Class<?> type, String value) 
	throws PlanBuilderException {
        
        try {
            if (type.equals(Byte.class)) return new Byte(value);
            else if (type.equals(Short.class)) return new Short(value);
            else if (type.equals(Character.class)) {
                if (value.length() != 1)
                    throw new PlanBuilderException("Could not build "
                                                   + "comparable value.");
                
                return new Character(value.charAt(0));
            }
            else if (type.equals(Integer.class)) return new Integer(value);
            else if (type.equals(Long.class)) return new Long(value);
            else if (type.equals(Float.class)) return new Float(value);
            else if (type.equals(Double.class)) return new Double(value);
            else if (type.equals(String.class)) return new String(value);
            else
                throw new PlanBuilderException("Could not build comparable "
                                               + "value.");
        }
        catch (Exception e) {
            throw new PlanBuilderException("Could not build comparable "
                                           + "value.", e);
        }
    } // createComparable()

    
    /**
     * Translator from logical to physical qualifications.
     * 
     * @param relationship the logical relationship between to values.
     * @return the physical relationship between the two values.
     */
    protected Condition.Qualification
        translateRelationship(Qualification.Relationship relationship) {
        
        switch (relationship) {
        case EQUALS:
            return Condition.Qualification.EQUALS;
        case NOT_EQUALS:
            return Condition.Qualification.NOT_EQUALS;
        case GREATER:
            return Condition.Qualification.GREATER;
        case GREATER_EQUALS:
            return Condition.Qualification.GREATER_EQUALS;
        case LESS:
            return Condition.Qualification.LESS;
        case LESS_EQUALS:
            return Condition.Qualification.LESS_EQUALS;
        }

        return Condition.Qualification.EQUALS;
    } // translateRelationship()
	
    protected Condition.Qualification
        reverseRelationship(Qualification.Relationship relationship) {
        
        switch (relationship) {
        case EQUALS:
            return Condition.Qualification.EQUALS;
        case NOT_EQUALS:
            return Condition.Qualification.NOT_EQUALS;
        case GREATER:
            return Condition.Qualification.LESS;
        case GREATER_EQUALS:
            return Condition.Qualification.LESS_EQUALS;
        case LESS:
            return Condition.Qualification.GREATER;
        case LESS_EQUALS:
            return Condition.Qualification.GREATER_EQUALS;
        }

        return Condition.Qualification.EQUALS;
    } // reverseRelationship()

    protected Qualification.Relationship
        reverseQR(Qualification.Relationship relationship) {
        
        switch (relationship) {
        case EQUALS:
            return Qualification.Relationship.EQUALS;
        case NOT_EQUALS:
            return Qualification.Relationship.NOT_EQUALS;
        case GREATER:
            return Qualification.Relationship.LESS;
        case GREATER_EQUALS:
            return Qualification.Relationship.LESS_EQUALS;
        case LESS:
            return Qualification.Relationship.GREATER;
        case LESS_EQUALS:
            return Qualification.Relationship.GREATER_EQUALS;
        }

        return Qualification.Relationship.EQUALS;
    } // reverseQR()

    
    /**
     * Enumerates the joins between a collection of sub-plans.
     * 
     * @param joins the logical joins to be enumerated.
     * @param branches the incoming sub-plans (branches).
     * @return a new vector of branches.
     * @throws PlanBuilderException thrown whenever join enumeration or
     * physical join construction is not possible.
     */
    protected List<Operator> enumerateJoins(List<Join> joins,
                                            List<Operator> branches)
	throws PlanBuilderException {
        
        try {
            if (branches.size() == 1 && joins.size() == 0) return branches;
            else if (branches.size() == 1 && joins.size() != 0)
                throw new PlanBuilderException("Could not enumerate joins");
            
            List<Operator> v = new ArrayList<Operator>();
			
            // pick a pair of branches and a predicate
            Triplet<Operator, Operator, List<Join>> triplet =
                pickPair(joins, branches);
            branches.remove(triplet.first);
            branches.remove(triplet.second);
            // build the join (?) operator
            if (triplet.third != null) {
                PhysicalJoin nlj = createJoin(triplet.first,
                                              triplet.second,
                                              triplet.third);
                List<Join> picked = triplet.third;
                for (Join it : picked) joins.remove(it);
                branches.add(nlj); 
            }
            else {
                // this is really a cartesian
                CartesianProduct cp = 
                    new CartesianProduct(triplet.first,
                                         triplet.second, sm);
                branches.add(cp);
            }
            
            return enumerateJoins(joins, branches);
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not enumerate joins.", ee);
        }
    } // enumerateJoins()
	
    /**
     * Given a collection of algebraic joins and a collection
     * of sub-plans, it picks a pair of branches to join.
     * 
     * @param joins a list of logical joins.
     * @param sources the incoming sub-plans.
     * @return a triplet consisting of the left and the right input
     * operators to the join, along with the algebraic join. The last
     * one is set to <code>null</code> if there are no more joins,
     * signifying that a Cartesian product should be built.
     * @throws PlanBuilderException thrown whenever a pair cannot be
     * picked.
     */
    protected Triplet<Operator, Operator, List<Join>>
        pickPair (List<Join> joins, List<Operator> sources) 
	throws PlanBuilderException {
        
        for (int i = 0; i < sources.size(); i++) {
            Operator left = sources.get(i);
            for (int j = i; j < sources.size(); j++) {
                Operator right = (Operator) sources.get(j);
                if (joined(left, right, joins))
                    return new Triplet<Operator, Operator, List<Join>>(
                        left, right, findJoins(left, right, joins));
            }
        }
		
        return new Triplet<Operator, Operator, List<Join>>(
            sources.get(0), sources.get(1), null);
    } // pickPair()
    
	
    /**
     * Given two operators (sub-plans) and a list of joins, it checks
     * if the two operators are joined.
     * 
     * @param left the left input.
     * @param right the right input.
     * @param joins a list of algebraic joins.
     * @return <code>true</code> if the sources are joined,
     * <code>false</code> otherwise.
     */
    protected boolean joined(Operator left, Operator right, List<Join> joins) 
	throws PlanBuilderException {
        
        for (Join join : joins) {
            // get the qualification
            VariableVariableQualification vvq =
                (VariableVariableQualification) join.getQualification();
            // get the two tables
            Variable leftVar = vvq.getLeftVariable();
            Variable rightVar = vvq.getRightVariable();
            String leftTable = leftVar.getTable();
            String rightTable = rightVar.getTable();
            // use getTables() to find the tables in each branch
            Set<String> leftTables = getTables(left);
            Set<String> rightTables = getTables(right);
            // check if these two tables are joined
            if ((leftTables.contains(leftTable) &&
                 rightTables.contains(rightTable))
                ||
                (leftTables.contains(rightTable) && 
                 rightTables.contains(leftTable)))
                return true;
        }
		
        return false;
    } // joined()

    
    /**
     * Given two operators and a collection of joins, return all joins
     * over the operators in a new vector.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param joins all joins between the two input operators.
     * @return a vector with all the joins between the two input
     * operators.
     * @throws PlanBuilderException thrown whenever joins cannot be
     * identified.
     */
    protected List<Join> findJoins(Operator left, Operator right,
                                   List<Join> joins) 
	throws PlanBuilderException {
        
        List<Join> out = new ArrayList<Join>();
        Set<String> leftTables = getTables(left);
        Set<String> rightTables = getTables(right);
        
        for (Join join : joins) {
            // get the qualification
            VariableVariableQualification vvq =
                (VariableVariableQualification) join.getQualification();
            // get the two tables
            Variable leftVar = vvq.getLeftVariable();
            Variable rightVar = vvq.getRightVariable();
            String leftTable = leftVar.getTable();
            String rightTable = rightVar.getTable();
            if ((leftTables.contains(leftTable) && 
                 rightTables.contains(rightTable)) ||
                (leftTables.contains(rightTable) && 
                 rightTables.contains(leftTable))) {
                out.add(join);
            }
        }
		
        return out;
    } // findJoins()

    
    /**
     * Given two input operators and a collection of algebraic joins,
     * returns a physical join to evaluate them.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param joins the joins between the two inputs.
     * @return a physical join to evaluate the join predicate.
     * @throws PlanBuilderException thrown whenever the physical join
     * cannot be instantiated.
     */
    protected PhysicalJoin createJoin(Operator left, 
                                      Operator right, 
                                      List<Join> joins) 
	throws PlanBuilderException {
        
        try {
            Predicate pred = null;
            boolean useFastJoin = false;
            
            if (joins.size() == 1) {
                Join join = joins.get(0);
                // single join, single predicate
                pred = createJoinPredicate(left, right, join);
                if (isMergeable(join)) useFastJoin = true;
            }
            else {
                // build a conjunction of all relevant predicates
                //
                // I have a feeling this will bite me in the ass when
                // dealing with multiple predicates over more than
                // three tables... (sviglas, 3/1/03)
                //
                // strange, it hasn't yet (sviglas, 4/1/08)
                //
                List<Predicate> preds = new ArrayList<Predicate>();
                for (Join join : joins)
                    preds.add(createJoinPredicate(left, right, join));
                pred = new Conjunction(preds);
            }

            if (! useFastJoin) {
                return new NestedLoopsJoin(left, right, sm, pred);
            }
            else {
                // a fast join algorithm can be used, so build the plan for it

                // create the various operations and
                // figure out which goes where
                                
                // sanity check
                                
                if (joins.size() != 1) {
                    throw new PlanBuilderException("Trying to build a sort-"
                                                   + "merge over a "
                                                   + "non-equi-join");
                }
                
                Join join = joins.get(0);

                /////////////////////////////////////////////
                //
                //  uncomment the following and comment out
                //  nested loops when you're done
                //
                //////////////////////////////////////////////
//*		
		Relation leftRel = left.getOutputRelation();
		Relation rightRel = right.getOutputRelation();
		VariableVariableQualification vvq =
		    (VariableVariableQualification) join.getQualification();
		Variable leftVar = vvq.getLeftVariable();
		Variable rightVar = vvq.getRightVariable();
		TupleSlotPointer leftTsp =
		    createSlotPointer(leftVar, leftRel);
		TupleSlotPointer rightTsp =
		    createSlotPointer(rightVar, rightRel);
		int [] leftSlots = new int[1];
		int [] rightSlots = new int[1];
		
		if (leftTsp == null) {
		    // could not find the left join input in
		    // the left-hand side
		    // it should be in the right-hand side
		    leftTsp = createSlotPointer(leftVar, rightRel);
		    rightTsp = createSlotPointer(rightVar, leftRel);
		    // if it is still null, then something's wrong
		    if (leftTsp == null) {
			throw new PlanBuilderException("Could not build "
						       + "fast join");
		    }
		    else {
			leftSlots[0] = rightTsp.getSlot();
			rightSlots[0] = leftTsp.getSlot();
		    }
		}
		else {
		    leftSlots[0] = leftTsp.getSlot();
		    rightSlots[0] = rightTsp.getSlot();
		}
		    
		/////////////////////////////////////////////////////
		//
		// Use your own implementation of sort here
		//
		/////////////////////////////////////////////////////
		if (parameters.get(Database.HASH_ALGORITHMS).equals("false")) {
		    int bufferPages = sm.getNumberOfBufferPoolPages();
		    int half = bufferPages / 2;
		    ExternalMergeSort newLeft =
			new ExternalMergeSort(left, sm, leftSlots,
					 half > 10 ? half : 10);
		    ExternalMergeSort newRight =
			new ExternalMergeSort(right, sm,
					 rightSlots, half > 10 ? half : 10);
		    // create the merge operation and combine it
		    pred = createJoinPredicate(newLeft, newRight, join);
		    return new MergeJoin(newLeft, newRight, sm, 
					 leftSlots[0], rightSlots[0], pred);
		}
		else {
		    pred = createJoinPredicate(left, right, join);
		    return new GraceHashJoin(left, right, sm,
					     leftSlots[0], rightSlots[0], 
					     sm.getNumberOfBufferPoolPages()/2 > 10 ? sm.getNumberOfBufferPoolPages()/2 : 10, 
					     pred);
		}
//*/
/*		
		pred = createJoinPredicate(left, right, join);
		return new NestedLoopsJoin(left, right, sm, pred);
//*/		
            }
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not instantiate physical "
                                           + "join", ee);
        }
    } // createJoin()


    /**
     * Checks whether sort-merge can be used or not.
     * 
     * @param join the logical join predicate to check
     * @return <code>true</code> if sort-merge can be used, 
     * <code>false</code> otherwise.
     */
    protected boolean isMergeable (Join join) {
        // basically, only equijoins for the time being
        // conjunctions could too be evaluated, but that
        // would make it all too complicated
        return join.getQualification().getRelationship() ==
            Qualification.Relationship.EQUALS;
    } // isMergeable()

    
    /**
     * Builds a single join predicate over two sources.
     * 
     * @param left the left input.
     * @param right the right input.
     * @param join the algebraic join.
     * @return the physical join predicate.
     * @throws PlanBuilderException thrown whenever the physical join
     * predicate cannot be instantiated.
     */
    protected Predicate createJoinPredicate(Operator left, 
                                            Operator right,
                                            Join join) 
	throws PlanBuilderException {
        
        try {
            VariableVariableQualification vvq =
                (VariableVariableQualification) join.getQualification();
            Variable leftVar = vvq.getLeftVariable();
            Variable rightVar = vvq.getRightVariable();
            Relation leftRel = left.getOutputRelation();
            Relation rightRel = right.getOutputRelation();
            TupleSlotPointer leftTsp = createSlotPointer(leftVar, leftRel);
            TupleSlotPointer rightTsp = createSlotPointer(rightVar, rightRel);
            Condition.Qualification qual =
                translateRelationship(vvq.getRelationship());
            // failed instantiation, we need to reverse the predicate
            if (leftTsp == null && rightTsp == null) {
                // this recursive call will succeed
                return createJoinPredicate(left, right,
                    new Join(
                        new VariableVariableQualification(reverseQR(
                            vvq.getRelationship()), rightVar, leftVar)));
            }
            return new TupleTupleCondition(leftTsp, rightTsp, qual);
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not obtain schema "
                                           + "information.", ee);
        }
    } // createJoinPredicate()

    
    /**
     * Given a list of algebraic operators, identify the projections
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators.
     * @return the projections in a new list.
     */
    protected List<Projection>
        getProjections(List<AlgebraicOperator> operators) {
        
        List<Projection> v = new ArrayList<Projection>();
        for (AlgebraicOperator alg : operators) 
            if (alg instanceof Projection) v.add((Projection) alg);
        
        return v;
    } // getProjections()

    
    /**
     * Given a list of algebraic operators, identify the selections
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators
     * @return the selections in a new list.
     */
    protected List<Selection> getSelections(List<AlgebraicOperator> operators) {

        List<Selection> v = new ArrayList<Selection>();
        for (AlgebraicOperator alg : operators)
            if (alg instanceof Selection) v.add((Selection) alg);

        return v;
    } // getSelections()

    
    /**
     * Given a list of algebraic operators, identify the joins
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators.
     * @return the joins in a new list.
     */
    protected List<Join> getJoins(List<AlgebraicOperator> operators) {
        
        List<Join> v = new ArrayList<Join>();
        for (AlgebraicOperator alg : operators)
            if (alg instanceof Join) v.add((Join) alg);

        return v;
    } // getJoins()

    /**
     * Given a list of algebraic operators, identify the sorts
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators.
     * @return the sorts in a new list.
     */
    protected List<Sort> getSorts(List<AlgebraicOperator> operators) {

        List<Sort> v = new ArrayList<Sort>();
        for (AlgebraicOperator alg : operators)
            if (alg instanceof Sort) v.add((Sort) alg);

        return v;
    } // getSorts()

    /**
     * Given a list of algebraic operators, identify the groupings
     * and return them in a new list.
     * 
     * @param operators the list of algebraic operators.
     * @return the groups in a new list.
     */
    protected List<Group> getGroups(List<AlgebraicOperator> operators) {
	List<Group> v = new ArrayList<Group>();
        for (AlgebraicOperator alg : operators)
            if (alg instanceof Group) v.add((Group) alg);

        return v;
    } // getGroups()

    
    /**
     * Imposes the final result projection.
     * 
     * @param projections the list of final projections.
     * @param operator the incoming operator.
     * @return the new operator with the projection imposed.
     * @throws PlanBuilderException thrown whenever the projection
     * cannot be instantiated.
     */
    protected Operator imposeFinalProjections(List<Projection> projections,
                                              Operator operator) 
	throws PlanBuilderException {
        try {
            List<Variable> pl = createProjectionList(projections);
            Relation relation = operator.getOutputRelation();
            int [] slots = convertProjectionList(pl, relation);
            Project p = new Project(operator, slots);
            return p;
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Could not instantiate final " +
                                           "projection.", ee);
        }
    } // imposeFinalProjections()


    /**
     * Imposes the final sort.
     *
     * @param sorts the list of sort operations.
     * @param operator the incoming top operator.
     * @throws PlanBuilderException thrown whenever the final sort
     * cannot be instantiated.
     */
    protected Operator imposeSorts(List<Sort> sorts, Operator operator)
        throws PlanBuilderException {
        try {
            if (sorts.size() > 1) {
                throw new PlanBuilderException("More than one sort clauses.");
            }
            else if (sorts.size() == 1) {                
                Sort sort = sorts.get(0);
                List<Variable> sl = sort.getSortList();
                Relation relation = operator.getOutputRelation();
                int [] slots = convertProjectionList(sl, relation);

                ///////////////////////////////////////////
                //
                // you should substitute the null
                // operator for external sort here
                //
                ///////////////////////////////////////////
                
                ///////////////////////////////////////////
                //
                // this is an example with 10 buffers
                //
                ///////////////////////////////////////////
                
                int bufferPages = sm.getNumberOfBufferPoolPages();
                int half = bufferPages / 2;
                ExternalSort es =
                    new ExternalSort(operator, sm, slots,
                                     half > 10 ? half : 10);
                return es;
                
                ///////////////////////////////////////////
                /*
                NullUnaryOperator nullop = new NullUnaryOperator(operator);
                return nullop;
                */
            }
            else {
                // just in case the sky falls
                return operator;
            }
                
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Cound not instantiate final "
                                           + "sort.", ee);
        }
    }

    /**
     * Imposes the final grouping.
     *
     * @param groups the list of group operations.
     * @param operator the incoming top operator.
     * @throws PlanBuilderException thrown whenever the final group
     * cannot be instantiated.
     */
    protected Operator imposeGroups(List<Group> groups, Operator operator)
        throws PlanBuilderException {
        try {
            if (groups.size() > 1) {
                throw new PlanBuilderException("More than one sort clauses.");
            }
            else if (groups.size() == 1) {                
                Group group = groups.get(0);
                List<Variable> sl = group.getGroupList();
                Relation relation = operator.getOutputRelation();
                int [] slots = convertProjectionList(sl, relation);

                ///////////////////////////////////////////
                //
                // you should substitute the null
                // operator for your implementation of
                // hash grouping here after checking whether
                // hash algorithms have been enabled or not
                //
                ///////////////////////////////////////////

		if (parameters.get(Database.HASH_ALGORITHMS).equals("true")) {
		    int bufferPages = sm.getNumberOfBufferPoolPages();
		    int half = bufferPages / 2;
		    HashGroup hg = new HashGroup(operator, sm, slots,
						 half > 10 ? half : 10);
		    return hg;
		}
		else {
		    // use sort-based grouping if hashing is not enabled
		    int bufferPages = sm.getNumberOfBufferPoolPages();
		    int half = bufferPages / 2;
		    ExternalSort es =
                    new ExternalSort(operator, sm, slots,
                                     half > 10 ? half : 10);
		    return es;
		}

                ///////////////////////////////////////////
            /*
                NullUnaryOperator nullop = new NullUnaryOperator(operator);
                return nullop;
            */
                
            }
            else {
                // just in case the sky falls
                return operator;
            }
                
        }
        catch (EngineException ee) {
            throw new PlanBuilderException("Cound not instantiate final "
                                           + "sort.", ee);
        }
    }
	

    /**
     * Given a list of projections, it collapses their projection
     * lists into a single one.
     * 
     * @param projections the list of algebraic projections.
     * @return a single projection list of all the projections lists
     * combined.
     */
    protected List<Variable>
        createProjectionList(List<Projection> projections) {

        // yeah yeah, I know, this isn't 1.5 generified and all that
        // crap...
        List<Variable> pl = projections.get(0).getProjectionList();
        for (int i = 1; i < projections.size(); i++) {
            Projection p = projections.get(i);
            pl.addAll(p.getProjectionList());
        }
	
        return pl;
    } // createProjectionList()
    
    /**
     * Debug main
     * 
     * @param args arguments
     */
    public static void main (String [] args) {
        try {
            org.dejave.attica.server.Database.ATTICA_DIR =
                System.getProperty("user.dir");
            String prefix = args[0] + System.getProperty("path.separator");
            // build the catalog
            Catalog catalog = new Catalog(prefix + "attica.cat");
            // build the buffer manager
            org.dejave.attica.storage.BufferManager bm = 
                new org.dejave.attica.storage.BufferManager(20);
            // build the storage manager
            StorageManager sm = new StorageManager(catalog, bm);
            // construct a database
            List<Attribute> attributes1 = new ArrayList<Attribute>();
            attributes1.add(new TableAttribute("table1", "key", Integer.class));
            attributes1.add(new TableAttribute("table1", "value",
                                               String.class));
            List<Attribute> attributes2 = new ArrayList<Attribute>();
            attributes2.add(new TableAttribute("table2", "key", Integer.class));
            attributes2.add(new TableAttribute("table2", "value",
                                               String.class));
            List<Attribute> attributes3 = new ArrayList<Attribute>();
            attributes3.add(new TableAttribute("table3", "key", Integer.class));
            attributes3.add(new TableAttribute("table3", "value",
                                               String.class));
            Table table1 = new Table("table1", attributes1);
            System.out.println(table1.getAttribute(0));
            Table table2 = new Table("table2", attributes2);
            Table table3 = new Table("table3", attributes3);
            org.dejave.attica.storage.CatalogEntry entry1 =
                new org.dejave.attica.storage.CatalogEntry(table1);
            org.dejave.attica.storage.CatalogEntry entry2 =
                new org.dejave.attica.storage.CatalogEntry(table2);
            org.dejave.attica.storage.CatalogEntry entry3 =
                new org.dejave.attica.storage.CatalogEntry(table3);
            catalog.createNewEntry(entry1);
            catalog.createNewEntry(entry2);
            catalog.createNewEntry(entry3);
            String filename1 = entry1.getFileName();
            sm.createFile(filename1);
            System.out.println(filename1 + " successfully created");
            String filename2 = entry2.getFileName();
            sm.createFile(filename2);
            System.out.println(filename2 + " successfully created");
            String filename3 = entry3.getFileName();
            sm.createFile(filename3);
            System.out.println(filename3 + " successfully created");
            org.dejave.attica.storage.RelationIOManager man1 = 
                new org.dejave.attica.storage.RelationIOManager(sm,
                                                                table1,
                                                                filename1);
            org.dejave.attica.storage.RelationIOManager man2 = 
                new org.dejave.attica.storage.RelationIOManager(sm,
                                                                table2,
                                                                filename2);
            org.dejave.attica.storage.RelationIOManager man3 = 
                new org.dejave.attica.storage.RelationIOManager(sm,
                                                                table3,
                                                                filename3);
            // populate it
            for (int i = 0; i < 100; i++) {
                List<Comparable> v1 = new ArrayList<Comparable>();
                v1.add(new Integer(i));
                v1.add(new String((v1.get(0)).toString()));
                org.dejave.attica.storage.Tuple tuple1 = 
                    new org.dejave.attica.storage.Tuple(
                        new org.dejave.attica.storage.TupleIdentifier(filename1,
                                                                      i),
                        v1);
                List<Comparable> v2 = new ArrayList<Comparable>();
                v2.add(new Integer(i % 5));
                v2.add(new String((v2.get(0)).toString()));
                org.dejave.attica.storage.Tuple tuple2 = 
                    new org.dejave.attica.storage.Tuple(
                        new org.dejave.attica.storage.TupleIdentifier(filename2,
                                                                      i),
                        v2);				
                List<Comparable> v3 = new ArrayList<Comparable>();
                v3.add(new Integer(i % 20));
                v3.add(new String((v3.get(0)).toString()));
                org.dejave.attica.storage.Tuple tuple3 = 
                    new org.dejave.attica.storage.Tuple(
                        new org.dejave.attica.storage.TupleIdentifier(filename3,
                                                                      i),
                        v3);
                man1.insertTuple(tuple1);
                //System.out.println("inserted " + tuple1);
                man2.insertTuple(tuple2);
                //System.out.println("inserted " + tuple2);
                man3.insertTuple(tuple3);
                //System.out.println("inserted " + tuple3);
            }
            // build an algebraic evaluation plan
            List<AlgebraicOperator> algebra =
                new ArrayList<AlgebraicOperator>();
            Join join1 =
                new Join(new VariableVariableQualification(
                    Qualification.Relationship.EQUALS,
                    new Variable("table1", "key"),
                    new Variable("table2", "key")));
            Join join2 = new Join(new VariableVariableQualification(
                    Qualification.Relationship.EQUALS,
                    new Variable("table2", "key"),
                    new Variable("table3", "key")));
            Selection sel = new Selection(new VariableValueQualification(
                    Qualification.Relationship.EQUALS,
                    new Variable("table3", "value"),
                    new String("0")));
            //Variable [] var = new Variable[2];
            List<Variable> pl = new ArrayList<Variable>();
            //var[0] = new Variable("table2", "value");
            //var[1] = new Variable("table1", "value");
            pl.add(new Variable("table3", "value"));
            Projection p = new Projection(pl);
            //algebra.addElement(sel);
            algebra.add(p);
            //algebra.addElement(join1); 
            //algebra.addElement(join2);
            System.out.println(algebra);
            // convert it to a physical plan
	    HashMap<String, String> map = new HashMap<String, String>();
	    map.put(Database.HASH_ALGORITHMS, "false");
            PlanBuilder pb = new PlanBuilder(catalog, sm, map);
            Operator operator = pb.buildPlan("lala", algebra);
            System.out.println(operator);
            // evaluate it
            System.out.println("evaluating...");
            System.out.println("results:");
            boolean done = false;
            while (! done) {
                org.dejave.attica.storage.Tuple tuple = operator.getNext();
                done = (tuple instanceof EndOfStreamTuple);
            }
        }
        catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()

} // PlanBuilder
