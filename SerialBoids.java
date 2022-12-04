import java.io.*;
import java.util.Scanner;   // Sigh
import java.util.StringTokenizer;
import java.util.ArrayList;

/* SerialBoids.java
*
*   Serial implementation of the Boids flocking algorithm, for
*   comparison to parallel implementation results
*
* written November 2022 as part of CSS 534 HW 5 by Thomas Pinkava
*/

public class SerialBoids {
    public static void main(String[] args){

        long startTime = System.nanoTime();  //Start a timer

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
            System.err.println("Usage: java serialBoids <initial condition filepath> <number of iterations>");
            System.exit(-1);
        }


        // Boidset is now initialized; run N simulation steps and output results to a file
        int timeSteps = 0;
        try{
            timeSteps = Integer.valueOf(args[1]);
        } catch (Exception exception){
            System.err.println("Usage: java serialBoids <initial condition filepath> <number of iterations>");
            System.exit(-1);
        }


        for(int i = 0; i < timeSteps; i++){
            // Output timestep to stdout
            for(Boid boid : boids){
                System.out.print(boid);
            }
            System.out.print("\n");

            // Update boids for next timestep
            // Copy boidset to act as input
            ArrayList<Boid> boidsCopy = new ArrayList<Boid>(boids.size());
            for(Boid boid : boids){
                boidsCopy.add(new Boid(boid.posX, boid.posY, boid.velX, boid.velY));
            }
            // Update all boids
            for(Boid boid : boids){
                boid.update(boidsCopy);
            }
        }
        
        long endTime = System.nanoTime();  // Stop timer
        System.err.println("Elapsed Time = " + (endTime - startTime) / 1.0e9);
    }
}
