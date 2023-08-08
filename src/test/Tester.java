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



public class Tester {

    /* ============================ GRAPH FIELDS ============================ */

    Label myLabel = Label.label("User");
    String graphName = "github.db";


    /* ============================ CONFIGURATION ============================ */

    final int lines_to_read = 10000;
    final boolean DEBUG = true;

    /* ============================ TRACE FILES ============================ */

    final String traceLocation = "operations/";
    final String intervalLocation = "traces/";

    /* ============================ PUBLIC CALLS ============================ */
    
    public void distributionTest(String graph_name, String file_name, String interval_file_name, int num_threads, int stride_size) {
	boolean outputFindTimes = true;
	this.graphName = graph_name;
	this.uniformTest(traceLocation + file_name, intervalLocation + interval_file_name, num_threads, stride_size, outputFindTimes);
    }
    
    /* ============================ TEST CODE ============================ */

    private void uniformTest(String operation_file_name, String interval_file_name, int num_threads, int stride_size, boolean output_find_times) {

	GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(this.graphName));
	graphDb.setParams(stride_size);
	graphDb.setNumThreads(num_threads);
	graphDb.setOperationDebugOutput(false);
	graphDb.setBatchingDebugOutput(false);
	graphDb.setFusionDebugOutput(false);

	try (Transaction tx = graphDb.beginTx()) {
	    Scanner operation_stream = new Scanner(new File(operation_file_name));
	    Scanner interval_stream = new Scanner(new File(interval_file_name));

	    int lines_read = 0;
	    long started_at = System.currentTimeMillis();

	    graphDb.beginPropagation();
	    // Submit ops
	    graphDb.startTimer();
	    while(operation_stream.hasNextLine() && lines_read < lines_to_read) {
		String operation;
		long interval;

		operation = operation_stream.nextLine();
		interval = Long.parseLong(interval_stream.nextLine());

		while(System.currentTimeMillis() < started_at + interval);
		try {
		    if(lines_read % 2500 == 0) {
			System.out.println("===== Submitted " + lines_read + " operations =====");
		    }
		    // System.out.println("===== Submit op " + lines_read + ": " + operation + " =====");
		    graphDb.parseLine(this.myLabel, operation);
		} catch (Exception e) {
		    System.out.println("Error parsing line: " + e);
		}
		lines_read++;
	    }

	    System.out.println("===== Operation Stream Finished =====");

	    graphDb.stopPropagation(); // should wait for graph to finish processing existing operations

	    long time = graphDb.stopTimer();
	    System.out.println(time / 1000.0);

	    tx.success();

	} catch (Exception e) {
	    System.out.println(e);
	}

	if(output_find_times) {
	    System.out.println("========== OPERATION TIMES ==========");
	    OperationTimeWrapper currotw = graphDb.getOpTimeWrapper();
	    if(currotw.opTimes.keySet().size() != lines_to_read) {
		System.out.println(" ===== TRIAL COMPLETED, BUT NOT ENOUGH TIMES IN MAP (" + currotw.opTimes.keySet().size() + "/" + lines_to_read + ")");
		for(long i = 0; i < 10000; i++) {
		    if(!currotw.opTimes.containsKey(i)) {
			System.out.println(i + " not found");
		    }
		}
	    }
	    else {
		for (Long key : currotw.opTimes.keySet()) {
		    System.out.println(key + ": " + currotw.opTimes.get(key).toString());
		}

		System.out.println("========== WAIT TIMES ==========");
		for(long i = 0; i < 10000; i++) {
		    if(currotw.waitTimes.containsKey(i)) {
			System.out.println(i + ": " + currotw.waitTimes.get(i).toString());
		    }
		    else {
			System.out.println(i + ": 0");
		    }
		}
	    }
	}

	graphDb.shutdown();
    }
    

