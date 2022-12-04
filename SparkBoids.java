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
*   NOTE: because the Spark library is fond of writing heaps of diagnostic
*         gibberish to stdout, output is piped to stderr instead, in reversal
*         of usual scheme.
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
            System.out.println("\nERROR: Submit initial condition file as first parameter\n");
            System.exit(-1);
            inputFileLines = null;  // To make the compiler happy
        }

        // Launch timer
        long startTime = System.nanoTime();


        // Parse boid strings into boid objects
        JavaRDD<Boid> boids = inputFileLines.map( s -> {
            String[] boidParams = s.split(",");

            Double posX = Double.valueOf(boidParams[0]);
            Double posY = Double.valueOf(boidParams[1]);
            Double velX = Double.valueOf(boidParams[2]);
            Double velY = Double.valueOf(boidParams[3]);

            return new Boid(posX, posY, velX, velY); 
        });

        
        // Enter into simulation loop
        int timeSteps = 0;
        if(args.length > 1){
            timeSteps = Integer.parseInt(args[1]);
        } else {
            System.out.println("\nERROR: Submit number of timesteps as second parameter\n");
            System.exit(-1);
        }

        for(int i = 0; i < timeSteps; i++){

            // 0) Collect and output time tick, to be piped into output file
            // TODO: this is the Achilles' heel of the algorithm in present state; too many Boids
            // returning to the master will cause memory overrun
            List<Boid> output = boids.collect();
            for(Boid boid : output){
                System.err.print(boid);
            }
            System.err.println("");



            // 1) Perform spatially hashed propagation: each Boid "receives" data from every neighbor in its
            //    Moore neighborhood (i.e. all the other Boids with which it could possibly be flockmates) and
            //    "emits" its own data to all the Moore neighborhoods of which it is a part
            //
            //  Dataset format: key, value pair where key is spatial region index and Value is a Tuple2 of
            //                  two sets of Boids: the first are the boids in the center of the region, the second are
            //                  all the boids in the region. Each boid emits nine messages, one of which contains itself
            //                  in the first set and the second set, the remainder of which only contain itself in the second set.
            JavaPairRDD<Integer, Tuple2<Set<Boid>,Set<Boid>>> propagatedBoids = boids.flatMapToPair( boid -> {
                List<Tuple2<Integer, Tuple2<Set<Boid>,Set<Boid>>>> flatOut = new ArrayList<>();
                
                // TODO spatial hash
                Set<Boid> core = new HashSet();
                core.add(boid);
                Set<Boid> surround = new HashSet();
                // TODO NOTE: Data flows from the surround set to the core set, so they have to be INDEPENDENT in memory.
                Boid dupe = new Boid(boid.posX, boid.posY, boid.velX, boid.velY);
                surround.add(dupe);
                flatOut.add(new Tuple2(Integer.valueOf(0), new Tuple2(core, surround)));

                return flatOut.iterator(); 
            });

           
 
            // 2) Collect neighborhoods together by key, combining the value boidsets. We therefore finish with
            //    one item per neighborhood, with a set of boids in its center, and a set of boids in its whole.
            
            propagatedBoids = propagatedBoids.reduceByKey((value1, value2) -> {
                value1._1.addAll(value2._1);
                value1._2.addAll(value2._2);
                return value1;
            });



            // 3) Update the boids in the center of each neighborhood with the data of the boids in the entire neighborhood.
            boids = propagatedBoids.values().flatMap(value -> {
 
                for(Boid boid : value._1){
                    // For every boid in the center of the neighborhood, add the effects of the other neighboring boids
                    boid.update(value._2);
                }
                return value._1.iterator();
            });
        }


        // Stop Spark
        sc.stop();

        // Stop timer and report
        System.out.println("Elapsed Time = " + (System.nanoTime() - startTime) / 1.0e9);

    }
}
