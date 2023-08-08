package test;

import org.neo4j.graphdb.GraphDatabaseService.OperationTimeWrapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class EagerTester {

    /* ============================ CONFIGURATION ============================ */

    Label myLabel = Label.label("User");
    String graphName = "github.db";

    final int lines_to_read = 10000;
    final boolean DEBUG = true;

    /* ============================ TRACE FILES ============================ */

    final String traceLocation = "operations/";
    final String intervalLocation = "traces/";

    /* ============================ PUBLIC CALLS ============================ */

    
    public void eagerDistributionTest(String graph_name, String file_name, String interval_file_name, int num_threads) {
	boolean outputFindTimes = true;
	this.graphName = graph_name;
	this.eagerUniformTest(traceLocation + file_name, intervalLocation + interval_file_name, num_threads, outputFindTimes);
    }

    
        ConcurrentHashMap<Long, Node> writing_nodes = new ConcurrentHashMap<Long, Node>();
    
    class EagerQueryRunner implements Runnable {
	GraphDatabaseService graph_db;
	String query;
	long operation_num;
	long should_have_been_submitted_at;
	EagerQueryRunner(GraphDatabaseService g, String q, long o, long s) {
	    this.graph_db = g;
	    this.query = q;
	    this.operation_num = o;
	    this.should_have_been_submitted_at = s;
	}
	public void run() {
	    try (Transaction tx = graph_db.beginTx()) {
		    try {
			this.graph_db.parseLineEager(myLabel, this.query, this.operation_num, System.currentTimeMillis() - this.should_have_been_submitted_at);
		    } catch (Exception e) {
			System.out.println("Got error " + e + " on query " + this.query);
		    }
		    tx.success();
		} catch (Exception e) {
		System.out.println("Error in run: " + e);
	    }
	}
    }

    class EagerDeleteRunner implements Runnable {
	GraphDatabaseService graph_db;
	String query;
	long operation_num;
	long should_have_been_submitted_at;
	EagerDeleteRunner(GraphDatabaseService g, String q, long o, long s) {
	    this.graph_db = g;
	    this.query = q;
	    this.operation_num = o;
	    this.should_have_been_submitted_at = s;
	}
	public void run() {
	    try (Transaction tx = graph_db.beginTx()) {
		    try {
			long start = System.currentTimeMillis();
			String[] tokens = this.query.split(" ");
			writing_nodes.put(this.operation_num, this.graph_db.findNodesTimed(myLabel, "userId", Integer.parseInt(tokens[1])).next());
			
		    } catch (Exception e) {
			System.out.println("Got error " + e + " on query " + this.query);
		    }
		    tx.success();
		} catch (Exception e) {
		System.out.println("Error in run: " + e);
	    }
	}
    }

    
    class EagerUpdateRunner implements Runnable {
	GraphDatabaseService graph_db;
	String query;
	long operation_num;
	long should_have_been_submitted_at;
	EagerUpdateRunner(GraphDatabaseService g, String q, long o, long s) {
	    this.graph_db = g;
	    this.query = q;
	    this.operation_num = o;
	    this.should_have_been_submitted_at = s;
	}
	public void run() {
	    try (Transaction tx = graph_db.beginTx()) {
		    try {
			String[] tokens = this.query.split(" ");
			writing_nodes.put(this.operation_num, this.graph_db.findNodesTimed(myLabel, tokens[1], Integer.parseInt(tokens[2])).next());
			
		    } catch (Exception e) {
			System.out.println("Got error " + e + " on query " + this.query);
		    }
		    tx.success();
		} catch (Exception e) {
		System.out.println("Error in run: " + e);
	    }
	}
    }

    
    private boolean isAdd(String op) {
	return op.indexOf("add") == 0;
    }

    private boolean isDelete(String op) {
	return op.indexOf("delete") == 0;
    }

    private boolean isUpdate(String op) {
	return op.indexOf("update") == 0;
    }

    private boolean isWrite(String op) {
	return isAdd(op) || isDelete(op) || isUpdate(op);
    }

    ThreadPoolExecutor thread_pool;
    private void eagerUniformTest(String operation_file_name, String interval_file_name, int num_threads, boolean output_find_times) {
	GraphDatabaseService graph_db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(this.graphName));
	thread_pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(num_threads);

	try {
	    Scanner operation_stream = new Scanner(new File(operation_file_name));
	    Scanner interval_stream = new Scanner(new File(interval_file_name));

	    int lines_read = 0;
	    long started_at = System.currentTimeMillis();
	    System.out.println("Reading " + lines_to_read + " lines");
	    System.out.println("Started at: " + started_at);
	    graph_db.startTimer();
	    while(operation_stream.hasNextLine() && lines_read < lines_to_read) {
		String operation = operation_stream.nextLine();
		long interval = Long.parseLong(interval_stream.nextLine());

		long now = System.currentTimeMillis();
		while(now - started_at < interval) {
		    now = System.currentTimeMillis();
		}
		long should_have_been_submitted_at = started_at + interval;
		
		if(isAdd(operation)) {
		    long start = System.currentTimeMillis();
		    String[] tokens = operation.split(" ");
		    String add_key = tokens[1];
		    int add_val = Integer.parseInt(tokens[2]);
		    String rel_key = tokens[3];
		    int rel_val = Integer.parseInt(tokens[4]);
		    String dir = tokens[5];
		    try (Transaction tx = graph_db.beginTx()) {
			Node returned = graph_db.findNodesTimed(myLabel, rel_key, rel_val).next();
			while(thread_pool.getActiveCount() != 0);
			Node created = graph_db.createNode(myLabel);
			created.setProperty(add_key, add_val);
			if(dir.equals("INCOMING") || dir.equals("BOTH")) {
			    returned.createRelationshipTo(created, RelationshipType.withName("FOLLOWS"));
			}
			if(dir.equals("OUTGOING") || dir.equals("BOTH")) {
			    created.createRelationshipTo(returned, RelationshipType.withName("FOLLOWS"));
			}
			long stop = System.currentTimeMillis();
			graph_db.op(lines_read, stop-start);
			graph_db.wait(lines_read, start-should_have_been_submitted_at);
			tx.success();
		    } catch (Exception e) {
			System.out.println(operation + ": " + e);
		    }
		}
		else if(isDelete(operation)) {
		    long start = System.currentTimeMillis();
		    try (Transaction tx = graph_db.beginTx()) {
			String[] tokens = operation.split(" ");
			Node writing_node = graph_db.findNodesTimed(myLabel, "userId", Integer.parseInt(tokens[1])).next();
			writing_node.setProperty("deleted", true);
			tx.success();
		    } catch (Exception e) {
			System.out.println(operation + ": " + e);
		    }
		    long stop = System.currentTimeMillis();
		    graph_db.op(lines_read, stop-start);
		    graph_db.wait(lines_read, start-should_have_been_submitted_at);
		}
		else if(isUpdate(operation)) {
		    try (Transaction tx = graph_db.beginTx()) {
			String[] tokens = operation.split(" ");
			long start = System.currentTimeMillis();
			Node writing_node = graph_db.findNodesTimed(myLabel, tokens[1], Integer.parseInt(tokens[2])).next();
			while(thread_pool.getActiveCount() != 0);
			writing_node.setProperty(tokens[3], Integer.parseInt(tokens[4]));
			long stop = System.currentTimeMillis();
			graph_db.op(lines_read, stop-start);
			graph_db.wait(lines_read, start-should_have_been_submitted_at);
			tx.success();
		    } catch (Exception e) {
			System.out.println(operation + ": " + e);
		    }

		}
		else {
		    thread_pool.execute(new EagerQueryRunner(graph_db, operation, lines_read, should_have_been_submitted_at));
		}
	    
		lines_read++;
		if(lines_read % 1000 == 0) {
		    System.out.println("Read line "  + lines_read + ": " + (System.currentTimeMillis() - started_at) / 1000.0);
		}
	    }
	    thread_pool.shutdown();
	    thread_pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	    long time = graph_db.stopTimer();
	    System.out.println(time / 1000.0);

	} catch (Exception e) {
	    System.out.println(e);
	}

	if (output_find_times) {

	    System.out.println("========== OPERATION TIMES ==========");
	    OperationTimeWrapper currotw = graph_db.getOpTimeWrapper();
	    for (Long key : currotw.opTimes.keySet()) {
		System.out.println(key + ": " + currotw.opTimes.get(key).toString());
	    }
	    System.out.println("========== WAIT TIMES ==========");
	    for (Long key : currotw.waitTimes.keySet()) {
		long time = currotw.waitTimes.get(key);
		System.out.println(key + ": " + (time > 0 ? time : 0));
	    }
	}

	graph_db.shutdown();
    }

}
