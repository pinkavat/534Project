package edu.uwb.css534;

import java.io.*;
import java.util.Scanner;   // Sigh
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import edu.uw.bothell.css.dsl.MASS.Agents;
import edu.uw.bothell.css.dsl.MASS.MASS;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;



/* BoidsMASS.java
*
*   TODO doc
*
* written December 2022 as part of CSS 534 HW 5 by Thomas Pinkava
*/


public class BoidsMASS{

    private static final String NODE_FILE = "nodes.xml";

    public static void main( String[] args ){

        // Start timer
        long startTime = System.nanoTime();
        
        // Read input parameters (code copied from SerialBoids.java)
        // Read an initial condition file and construct a set of Boids
        ArrayList<Boid> boids = new ArrayList<Boid>();
        
        try{

            // Open initial condition file
            File initialConditionFile = new File(args[0]);
            Scanner scan = new Scanner(initialConditionFile);

            // Convert lines to boids
            while(scan.hasNextLine()){
                // Convert line to boid
                String boidString = scan.nextLine();
                String[] boidParams = boidString.split(",");

                Double posX = Double.valueOf(boidParams[0]);
                Double posY = Double.valueOf(boidParams[1]);
                Double velX = Double.valueOf(boidParams[2]);
                Double velY = Double.valueOf(boidParams[3]);

                boids.add(new Boid(posX, posY, velX, velY));
            }

        } catch (FileNotFoundException exception){
            System.err.println("File not found");
            System.exit(-1);
        } catch (Exception exception){
            System.err.println("Usage: java BoidsMASS <initial condition filepath> <number of iterations>");
            System.exit(-1);
        }



        // Initialize MASS
        MASS.setNodeFilePath(NODE_FILE);
        MASS.setLoggingLevel(LogLevel.DEBUG);   // TODO: change to ERROR for benchmarking!
    
        // Start MASS
        MASS.getLogger().debug("Boids: Initializing MASS Library");
        MASS.init();
        MASS.getLogger().debug("Boids: MASS Library Initialized");




        // Generate place (TODO place*s*)
        MASS.getLogger().debug( "Boids: Creating Places..." );
        Places places = new Places( 1, BoidPlace.class.getName(), null, 1, 1);    // TODO many
        MASS.getLogger().debug( "Boids: Places created" );

        // Generate agents
        MASS.getLogger().debug( "Boids: Creating Agents..." );
        Agents agents = new Agents(1, BoidAgent.class.getName(), null, places, boids.size());
        MASS.getLogger().debug( "Boids: Agents created" );

        // Populate agents with Boid data
        MASS.getLogger().debug( "Boids: Adding data to agents" );
        agents.callAll(BoidAgent.init_, boids.toArray());
        MASS.getLogger().debug( "Boids: Data added to agents" );




        // Boidset is now initialized; run N simulation steps and output results to a file
        int timeSteps = 0;
        try{
            timeSteps = Integer.valueOf(args[1]);
        } catch (Exception exception){
            System.err.println("Usage: java BoidsMASS <initial condition filepath> <number of iterations>");
        }
        for(int i = 0; i < timeSteps; i++){
            // For each timestep:

            // 1) Each BoidSpace gathers together its Boidset (and returns it, for printing)
            Object[] boidDataSets = places.callAll(BoidPlace.getOwnBoids_, null);
            //  aggregate all boidsets...
            Set<Boid> allBoids = new HashSet<Boid>();
            for(Object subSet : boidDataSets){
                if(subSet != null){
                    allBoids.addAll((Set<Boid>)subSet);
                }
            }
            // ...and output timestep:
            for(Boid b : allBoids){
                System.out.print(b);
            }
            System.out.println("");
            

            // 2) Each BoidSpace exchanges its Boidset with its Moore Neighborhood
            places.exchangeAll(1, BoidPlace.exchangeBoids_);
            places.callAll(BoidPlace.aggregateBoids_, null);

            // 3) Each BoidAgent is provided with the Boidset of the Moore Neighborhood of its BoidSpace
            //    and updates accordingly
            agents.callAll(BoidAgent.updateBoid_, null);

            // 4) Each BoidAgent migrates to a new BoidSpace, if necessary.
            agents.manageAll();

            if((i % 5) == 0) // Unbelievable, Java!
                System.err.println("Completed tick " + i);
        }


        // End MASS
        MASS.getLogger().debug("Boids: Shutting down MASS Library");
        MASS.finish();
        MASS.getLogger().debug("Boids: MASS Library stopped");

        // Stop timer and report
        System.err.println("Elapsed Time = " + (System.nanoTime() - startTime) / 1.0e9);


    }
}
