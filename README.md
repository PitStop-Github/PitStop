# PitStop

The goal of PitStop is to use lazy evaluation to improve both throughput and operation latency for data processing systems. PitStop allows for queries and updates to be temporarily halted mid-traversal and continued at a later time. Though it may seem counter intuitive, this pausing allows for a fine-grained parallelism that helps to balance workload and alleviate workload fluctuation. This design also allows for further query optimization through the use of techniques such as *batching* and *fusion*.

This repository contains the source code and tester code used to run the experiments. Instructions for importing the Twitter dataset used in the paper are also included, as well as sample traces for both the operation streams and input intervals.
   
## Running the Prototype

### Dependencies:

- Linux machine
- Maven 3.6+
- Java 8
- A valid Neo4j graph (see [here](https://github.com/PitStop-Github/PitStop/tree/master/src/twitter-dataset) for instructions on how to download and convert the Twitter data we used)

### To run the code:

1. Clone the repository.
2. Navigate to the directory of the prototype version you would like to run (```src/neo4j-PitStop``` or ```src/neo4j-bmp```).
3. Compile the project with Maven using 
   ```        
   mvn clean install -e -DskipTests -Dcheckstyle.skip -Dlicensing.skip
4. Navigate to ```packaging/standalone/target``` within the directory and un-tar the compiled ```neo4j-community-3.5.6-SNAPSHOT-unix.tar.gz```.
5. Navigate to the ```test``` directory, and update the ```lines_to_read```, ```traceLocation``` and ```intervalLocation``` to your specific values in both ```Tester.java``` and ```EagerTester.java```.
6. Compile the tester code with
   ```
   javac -cp .:/DIRECTORY/neo4j-community-3.5.6-SNAPSHOT/lib/* -d bin Tester.java
   javac -cp .:/DIRECTORY/neo4j-community-3.5.6-SNAPSHOT/lib/* -d bin EagerTester.java
   javac -cp .:bin -d bin LazyTest.java
   javac -cp .:bin -d bin EagerTest.java
   ```
   where ```DIRECTORY``` represents the absolute path to the location of the extracted folder from step 4.
6. Navigate to the generated ```bin/``` folder.
7. For ```neo4j-PitStop``` or  ```neo4j-bmp```, run
   ```
   java -Xmx4g -cp .:/home/Project/src/neo4j/packaging/standalone/target/neo4j-community-3.5.6-SNAPSHOT/lib/* test.LazyTest <graphLocation> <operationStreamFile> <intervalFile> <numThreads> <strideSize>
   ```
   If using ```neo4j-bmp```, make the ```strideSize``` the total number of nodes in the graph.
   
8. For an eager (MP) comparison, run
      ```
   java -Xmx4g -cp .:/home/Project/src/neo4j/packaging/standalone/target/neo4j-community-3.5.6-SNAPSHOT/lib/* test.EagerTest <graphLocation> <operationStreamFile> <intervalFile> <numThreads>
   ```
   
   
   Sample interval files and opeartion stream files can be found in ```src/traces```.  
   Output will be in the format of ```<operationNumber> : <time (ms)>```
