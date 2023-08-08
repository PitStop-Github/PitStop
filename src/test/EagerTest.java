package test;

public class EagerTest {
    public static void main(String[] args) {
	EagerTester t = new EagerTester();

	String graph_name = args[0];
	String file_name = args[1];
	String interval_file_name = args[2];
	int num_threads = Integer.parseInt(args[3]);
	
	System.out.println(" ========== " +
			   " Eager test: (" +
			   " graph: " + graph_name + " " +
			   num_threads + " threads, " +
			   "interval file " + interval_file_name + ", " +
			   "file " + file_name + ")" + 
			   " ========== ");
	
	t.eagerDistributionTest(graph_name, file_name, interval_file_name, num_threads);
    }
}
