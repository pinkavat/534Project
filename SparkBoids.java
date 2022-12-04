import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import java.util.*;
import java.io.*;
import scala.Tuple2;


/* SparkBoids.java
*
*   Spark-based implementation of the Boids flocking algorithm
*   Uses the Boid class (Boid.java) for the actual core algorithm, with 
*   Spark being responsible for a spatial-hashing-based reduction
*   in comparisons
*
* written December 2022 as part of CSS 534 HW 5 by Thomas Pinkava
*/


public class SparkBoids {

    

    public static void main(String[] args) {

        // Start Spark
        SparkConf sparkConf = new SparkConf()
            .setAppName("Boids Flocking Sim -- Spark Variant");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        

        // Read input file into RDD; input format is one boid (four doubles, comma separated)
        // per line.
        JavaRDD<String> inputFileLines;
        try {
            String inputFile = args[0];
            inputFileLines = sc.textFile(inputFile);
        } catch (Exception exception){
            System.err.println("\nERROR: Submit initial condition file as first parameter\n");
            System.exit(-1);
        }

        // Launch timer
        long startTime = System.currentTimeMillis();







        // Stop Spark
        sc.stop();

        // Stop timer and report
        System.err.println("Elapsed Time: " + (System.currentTimeMillis() - startTime));

    }
}



    /* TODO DELETE: OLD CODE FOR REFERENCE
    // Our own redefinition of the Data class; a wrapper for a thruple of
    // neighbors, distance to source, and active status
    private static class Data implements Serializable{
        List<Tuple2<Integer, Integer>> neighbors;   // <neighbor index, distance> edge list from graph file
        Integer distance;                           // distance to source from this vertex
        Boolean active;                             // active status of vertex

        public Data(){
            neighbors = new ArrayList<Tuple2<Integer, Integer>>();
            distance = Integer.MAX_VALUE;
            active = new Boolean(false);
        }

        public Data(List<Tuple2<Integer, Integer>> neighbors, Integer distance, Boolean active){
            this.neighbors = neighbors;
            this.distance = distance;
            this.active = active;
        }
    }


 
    public static void main(String[] args) {

        // Start Spark
        SparkConf sparkConf = new SparkConf()
            .setAppName("BFS-Based Shortest Path Search");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        

        // Read input file into RDD
        String inputFile = args[0];
        JavaRDD<String> lines = sc.textFile( inputFile );

        // Launch timer
        long startTime = System.currentTimeMillis();


        // Convert input lines to "Data set" :)
        JavaPairRDD<Integer, Data> network = lines.mapToPair(
            line -> {
                String[] splitLine = line.split("[=,;]");
                ArrayList<Tuple2<Integer,Integer>>neighbors = new ArrayList<Tuple2<Integer,Integer>>();

                for(int i = 1; i < splitLine.length; i+= 2){
                    neighbors.add(new Tuple2<Integer, Integer>(Integer.valueOf(splitLine[i]), Integer.valueOf(splitLine[i + 1])));
                }
 
                return new Tuple2(
                    Integer.valueOf(splitLine[0]), 
                    new Data(
                        neighbors, 
                        (splitLine[0].equals(args[1])) ? new Integer(0) : Integer.MAX_VALUE,    // Source vertex is 0 distance
                        new Boolean(splitLine[0].equals(args[1]))                               // Source vertex is initially active
                    )
                );
            }
        );


        // Run the propagation Loop
        do {
            // While active vertices remain 
            
            // All vertices regenerate themselves; active vertices "burst" their distance sets to all neighbors.
            JavaPairRDD<Integer, Data> propagatedNetwork = network.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Integer, Data>, Integer, Data>(){
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Integer, Data>> call(Tuple2<Integer, Data> vertex){
                        List<Tuple2<Integer, Data>> output = new ArrayList<>();

                        // Always regenerate the self (inactivated)
                        output.add(new Tuple2(vertex._1, new Data(vertex._2.neighbors, vertex._2.distance, new Boolean(false))));
                        // We identify the original vertex in the recombination by whether or not it has
                        // a non-null neighborset

                        // If the vertex is active, perform a burst
                        if(vertex._2.active.booleanValue()){
                            for(Tuple2<Integer, Integer> neighbor : vertex._2.neighbors){

                                // Dispatch message to each neighbor
                                output.add(new Tuple2(neighbor._1, new Data(null, neighbor._2.intValue() + vertex._2.distance, new Boolean(false))));
                            }
                        }

                        return output.iterator();
                    }
                }
            );


            network = propagatedNetwork.reduceByKey( (data1, data2) ->{
                // Unite the two distances. If the original vertex (the one with non-null neighbors) got improved,
                // activate it.
                boolean improved = false;
                Integer newDist;
                boolean leftActualVertex = data1.neighbors != null;
                boolean rightActualVertex = data2.neighbors != null;

                if(data1.distance < data2.distance){
                    newDist = data1.distance;
                    improved = rightActualVertex;
                } else {
                    // Assumption: edge weights always greater than zero
                    newDist = data2.distance;
                    improved = leftActualVertex;
                }
    
                return new Data(
                    leftActualVertex ? data1.neighbors : data2.neighbors,
                    newDist,
                    new Boolean(data1.active.booleanValue() || data2.active.booleanValue() || improved)
                );
            });

        } while(
            network.values().reduce(
                (a, b) -> {return new Data(null, null, a.active.booleanValue() || b.active.booleanValue());}
            ).active.booleanValue()
            // TODO: make redundant!
        );


        // Collect result (distance to target vertex)
        List<Data> result = network.lookup(new Integer(args[2]));
        System.err.println("Distance from vertex " + args[1] + " to vertex " + args[2] + " = " + result.get(0).distance);
        
        // Stop Spark
        sc.stop();

        // Stop timer and report
        System.err.println("Elapsed Time: " + (System.currentTimeMillis() - startTime));
        */
