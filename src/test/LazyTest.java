package test;

public class LazyTest {
    public static void main(String[] args) {
	Tester t = new Tester();
	
	String graph_name = args[0];
	String file_name = args[1];
	String interval_file_name = args[2];
	int num_threads = Integer.parseInt(args[3]);
	int stride_size = Integer.parseInt(args[4]);
	
	System.out.println(" ========== " +
			   " Lazy test: (" +
			   " graph: " + graph_name + " " +
			   num_threads + " threads, " +
			   "interval file " + interval_file_name + ", " +
			   "stride size " + stride_size + ", " +  
			   "file " + file_name + ")" +
			   " ========== ");
	
	t.distributionTest(graph_name, file_name, interval_file_name, num_threads, stride_size);
    }
}
