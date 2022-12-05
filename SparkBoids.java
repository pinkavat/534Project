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

        // Algorithm requires Boid flocking radius; however, it's stored in Boid.java as a squared
        // radius, so we precompute the square root here for modularity.
        double FLOCK_RADIUS = Math.sqrt(Boid.FLOCK_RADIUS_SQUARED) + 2.0;   // Add a bit of slop for safety's sake
                                                                            // one never can tell with floats

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


            // ========== BEGIN NAIVE APPROACH (because spatial hash is causing hangs) ==========
           
            /* 
            JavaRDD<Boid> others = boids.map(boid -> {
                return new Boid(boid.posX, boid.posY, boid.velX, boid.velY);
            });
        
            JavaPairRDD<Boid, Boid> allPairs = boids.cartesian(others);
            
            boids = allPairs.map(boidSetPair ->{
                ArrayList<Boid> b = new ArrayList<>(1);
                b.add(boidSetPair._2);
                boidSetPair._1.update(b);
                return boidSetPair._1;
            });
            */
            /*
            JavaRDD<Set<Boid>> others = boids.map(boid -> {
                Set<Boid>out = new HashSet<>(1);
                out.add(new Boid(boid.posX, boid.posY, boid.velX, boid.velY));
                return out;
            });

            JavaPairRDD<Boid, Set<Boid>> allPairs = boids.cartesian(others);

            allPairs = allPairs.reduceByKey((value1, value2) -> {
                value1.addAll(value2);
                return value1;
            });

            allPairs.foreach(boidSetPair -> {
                boidSetPair._1.update(boidSetPair._2);
            });

            boids = allPairs.keys();
            */
        
            // ========== BEGIN NONFUNCTIONAL SPATIAL HASHING APPROACH ==========
            
            // Begin spatial hashing by collecting the central chunks of each superchunk together, to
            // make chunk traffic on a per-chunk rather than a per-boid basis
            JavaPairRDD<Tuple2<Integer, Integer>, Set<Boid>> combinedCores = boids.flatMapToPair( boid -> {

                List<Tuple2<Tuple2<Integer, Integer>, Set<Boid>>> flatOut = new ArrayList<>(1);

                // Spatial hashing algorithm: space is divided into square chunks of flocking radius size, such that
                // a Boid in a chunk cannot be flockmates with Boids in chunks outside the Moore Neighborhood of said chunk
                // We therefore establish which chunk by dividing the Boid's position by the flock radius:
                int chunkX = (int)(boid.posX / FLOCK_RADIUS);
                int chunkY = (int)(boid.posY / FLOCK_RADIUS);

                Set<Boid> core = new HashSet<>();
                core.add(boid);

                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX), Integer.valueOf(chunkY)), core));

                return flatOut.iterator();
            });


            // Combine the core sets (simple append)
            combinedCores = combinedCores.reduceByKey( (value1, value2) -> {
                value1.addAll(value2);
                return value1;
            });


            //    Perform spatially hashed propagation: each Boid "receives" data from every neighbor in its
            //    Moore neighborhood (i.e. all the other Boids with which it could possibly be flockmates) and
            //    "emits" its own data to all the Moore neighborhoods of which it is a part
            //
            //  Dataset format: key, value pair where key is spatial region indexing tuple and Value is a tuple of
            //                  two sets of Boids: the first are the boids in the center of the region, the second are
            //                  all the boids in the region. Each boid emits nine messages, one of which contains itself
            //                  in the first set and the second set, the remainder of which only contain itself in the second set.
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Set<Boid>,Set<Boid>>> propagatedBoids = combinedCores.flatMapToPair(kvPair -> {
                
                List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Set<Boid>,Set<Boid>>>> flatOut = new ArrayList<>();

                Integer chunkX = kvPair._1._1;
                Integer chunkY = kvPair._1._2;

                // Duplicate message set
                Set<Boid> dupe = new HashSet(kvPair._2.size());
                for(Boid boid : kvPair._2){
                    dupe.add(new Boid(boid.posX, boid.posY, boid.velX, boid.velY));
                }

                // The "core set" is the central chunk of the 9x9 chunks of the spatial hash; it contains 'true' boids,
                // that receive data.
                flatOut.add(new Tuple2(kvPair._1, new Tuple2(kvPair._2, dupe)));
                                
                // We create a dummy empty core for the messages to neighboring chunksets
                Set<Boid> dummy = new HashSet();
                
                // Moore neighborhood
                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX-1), Integer.valueOf(chunkY-1)),new Tuple2(dummy, dupe)));
                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX-1), Integer.valueOf(chunkY)),  new Tuple2(dummy, dupe)));
                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX-1), Integer.valueOf(chunkY+1)),new Tuple2(dummy, dupe)));
                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX+1), Integer.valueOf(chunkY-1)),new Tuple2(dummy, dupe)));
                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX+1), Integer.valueOf(chunkY)),  new Tuple2(dummy, dupe)));
                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX+1), Integer.valueOf(chunkY+1)),new Tuple2(dummy, dupe)));
                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX),   Integer.valueOf(chunkY-1)),new Tuple2(dummy, dupe)));
                flatOut.add(new Tuple2(new Tuple2(Integer.valueOf(chunkX),   Integer.valueOf(chunkY+1)),new Tuple2(dummy, dupe)));

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
