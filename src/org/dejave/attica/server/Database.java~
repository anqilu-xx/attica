/*
 * Created on Dec 14, 2003 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project. Any subsequent modification of
 * the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.server;

import java.io.Reader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;

import java.util.Properties;
import java.util.List;

import org.dejave.util.Args;

import org.dejave.attica.engine.algebra.AlgebraicOperator;
import org.dejave.attica.engine.operators.EngineException;
import org.dejave.attica.engine.operators.EndOfStreamTuple;
import org.dejave.attica.engine.operators.Operator;
import org.dejave.attica.engine.operators.MessageSink;
import org.dejave.attica.engine.operators.Sink;

import org.dejave.attica.engine.optimiser.PlanBuilder;

import org.dejave.attica.model.TableAttribute;
import org.dejave.attica.model.Table;

import org.dejave.attica.sql.parser.SQLParser;

import org.dejave.attica.storage.BufferManager;
import org.dejave.attica.storage.Catalog;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;

import org.dejave.attica.storage.FileUtil;


/**
 * Database: The database driver file.
 *
 * @author sviglas 
 */
public class Database {
	
    /** The DB properties. */
    private Properties props;
	
    /** Directory of the server. */
    public static String ATTICA_DIR;
    
    /** Temporary directory. */
    public static String TEMP_DIR;
    
    /** The user prompt. */
    private static final String PROMPT = "aSQL > ";
    
    /** Number of pages in the buffer pool. */
    private int bufferSize;
    
    /** DB catalog. */
    private Catalog catalog;
    
    /** The storage manager instance. */
    private StorageManager sm;
	
    /** The buffer manager instance. */
    private BufferManager bm;
	
    /** SQL Parser. */
    private SQLParser parser = null;
	
    /**
     * Constructs a new Database instance using the given file name to
     * read the properties from.
     * 
     * @param propsFile the name of the file containing the DB
     * properties.
     * @throws ServerException thrown whenever the DB cannot be
     * constructed.
     */
    public Database(String propsFile) throws ServerException {
        this(propsFile, false);
    } // Database()
	
    /**
     * Constructs a new Database specifying whether this is a new
     * instance or not.
     * 
     * @param propsFile the name of the file containing the DB
     * properties.
     * @param init if set to <code>true</code> this is a new DB
     * instance, so the catalog is generated, if set to
     * <code>false</code> this is an old instance and the catalog is
     * simply read from disk.
     * @throws ServerException thrown whenever the DB cannot be
     * constructed.
     */
    public Database(String propsFile, boolean init) throws ServerException {
        
        try {
            System.out.println("Starting up the database...");
            System.out.println("Bootstrapping: Loading properties from "
                               + propsFile);
            FileInputStream fis = new FileInputStream(propsFile); 
            props = new Properties();
            props.load(fis); 
            fis.close(); 
            ATTICA_DIR = props.getProperty("attica.directory", 
                                           System.getProperty("user.dir")).trim();
            TEMP_DIR = props.getProperty("attica.temp.directory",
                                         System.getProperty("user.dir")).trim();
            bufferSize = 
	      Integer.parseInt(props.getProperty("attica.buffersize", "50").trim());
            
            // start up the new buffer manager
            bm = new BufferManager(bufferSize);
            // start up the catalog
            String catalogFile = ATTICA_DIR
                + System.getProperty("file.separator") + "attica.catalog";
            catalog = new Catalog(catalogFile);
            // don't look at me if you're stupid, blame your genes...
            if (init) catalog.writeCatalog();
            else catalog.readCatalog();
            // start up the storage manager
            sm = new StorageManager(catalog, bm);
			
            // we're all ready now...
            System.out.println("Attica server is running...");
            System.out.println("Data directory: " + ATTICA_DIR);
            System.out.println("Termporary directory: " + TEMP_DIR);
            System.out.println("Buffer pool size: " + bufferSize + " pages");
            System.out.println("** ready **");
            System.out.print(PROMPT);
        }
        catch (Exception e) {
            throw new ServerException("Could not initialise the database.", e);
        }
    } // Database()

    
    /**
     * Executes the given statement.
     * 
     * @param statement the statement to be executed.
     * @return a Sink operator for result retrieval.
     * @throws EngineException thrown whenever the statement cannot be 
     * executed.
     */
    public Sink runStatement(String statement) throws EngineException {
        
        // @#$%ing static parsers...
        if (parser == null) {
            //parser = new SQLParser(statement, catalog);
            parser =
                new SQLParser(new ByteArrayInputStream(statement.getBytes()));
            parser.setCatalog(catalog);
        }
        else {
            SQLParser.
                ReInit(new ByteArrayInputStream(statement.getBytes()));
        }
        
        Statement result = null;
		
        try {
            result = SQLParser.Start();
            // this is a query -- call the plan builder and run it
            if (result instanceof Query) {
                Query q = (Query) result;
                List<AlgebraicOperator> algebra = q.getAlgebra();
                PlanBuilder pb = new PlanBuilder(catalog, sm);
                String file = FileUtil.createTempFileName();
                Operator operator = pb.buildPlan(file, algebra);
                return (Sink) operator;
            }
            // this is a new table -- insert it into the database
            else if (result instanceof TableCreation) {
                TableCreation tc = (TableCreation) result;
                Table table = tc.getTable();
                sm.createTable(table);
                catalog.writeCatalog();
                MessageSink ms = new MessageSink("Table " + table.getName() +
                                                 " successfully created");
                return ms;
            }
            // this is a drop
            else if (result instanceof TableDeletion) {
                TableDeletion td = (TableDeletion) result;
                String tablename = td.getTableName();
                sm.deleteTable(tablename);
                MessageSink ms = new MessageSink("Table " + tablename +
                                                 " successfully dropped");
                return ms;
            }
            // this is a tuple insertion -- make it happen
            else if (result instanceof TupleInsertion) {
                TupleInsertion ti = (TupleInsertion) result;
                String table = ti.getTableName();
                List<Comparable> values = ti.getValues();
                sm.castAndInsertTuple(table, values);
                MessageSink ms = new MessageSink("Tuple was successfully "
                                                 + "inserted into table "
                                                 + table);
                return ms;
            }
            // show the DB catalog
            else if (result instanceof ShowCatalog) {
                MessageSink ms = new MessageSink(catalog.toString());
                return ms;
            }
            // show the table attributes
            else if (result instanceof TableDescription) {
                TableDescription td = (TableDescription) result;
                String name = td.getTableName();
                Table table = catalog.getTable(name);
                MessageSink ms = new MessageSink(table.toString());
                return ms;
            }
        }
        catch (Exception e) {
            MessageSink ms = new MessageSink(e.getMessage());
            return ms;
        }
		
        MessageSink ms = new MessageSink("Could not run query");
        return ms;
    } // runQuery()

    
    /**
     * Shuts down the server.
     * 
     * @throws ServerException thrown whenever the server cannot be shut
     * down.
     */
    public void shutdown() throws ServerException {
        
        try {
            // dump the catalog
            catalog.writeCatalog();
            // shut down the storage manager -- it'll flush the buffer
            // pool too; I know this is not the best encapsulation
            // ever, but de-coupling them at this point will probably
            // cause a rewrite of parts of the pool and the storage
            // manager that I don't want to touch right now. (by the
            // way, what the hell was I thinking five years ago?)
            sm.shutdown();
        }
        catch (StorageManagerException sme) {
            throw new ServerException("Could not shut down the server", sme);
        }    
    } // shutDown()

    
    /**
     * Start up the database.
     * 
     * @param args the arguments to the server.
     */
    public static void main (String [] args) {
        
        try {
            // parse arguments
            if (Args.gettrig(args, "--help")) {
                System.err.println("Usage: java attica.server.Database "
                                   + "<attica-properties-file> [--init]");
                System.exit(0);
            }

            // load the properties
            String properties =
                Args.getopt(args, "--properties", "attica.properties");
            // should we initialise the database?
            boolean init = Args.gettrig(args, "--init");
            // construct the db instance
            Database db = new Database(properties, init);
            // the input stream
            Reader in = new InputStreamReader(System.in);

            // and now the simplest prompt loop in the history of
            // database prompt loops. actually, simple doesn't quite
            // describe it. some people might even call it lame. I
            // would agree with those people. I do understand that I'm
            // calling myself lame, but at least I have the
            // self-knowledge to do so. so let me repeat: this is
            // lame. in fact, if you have a minute, it is an eight
            // storey high temple of lameness, with a big flashing
            // neon sign on top saying "this is lame".
            boolean done = false;
            while (! done) {
                StringBuffer sb = new StringBuffer();
                char c;
                // yeah, so we need to call a method just to append a
                // character... isn't java lovely?
                while ((c = (char) in.read()) != ';') sb.append(c);
                
                String input = sb.toString();
                done = input.trim().equals("exit");
                if (! done) {
                    Sink sink = db.runStatement(input);

                    for (Tuple tuple : sink.tuples())
                        System.out.println(tuple.toStringFormatted());

                    System.out.print(PROMPT);
                }
            }
            db.shutdown();
        }
        catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()

} // Database
