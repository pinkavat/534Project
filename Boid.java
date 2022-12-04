import java.io.Serializable;

/* Boid.java
*
*   Class for a single "Boid" (flocking particle) object.
*   Since all three of our parallelization programs use Java, this
*   is an obvious simplification.
*
* written November 2022 as part of CSS 534 HW 5
*/

public class Boid implements Serializable {
    // Serializable, to be pushed down network pipes by the 
    // parallelization libraries


    // ========== SIMULATION PARAMETERS (shared between all boids) ==========

    static double FLOCK_RADIUS_SQUARED = 4900.0;    // Radius squared of local flock in units squared

    static double TOO_CLOSE_SQUARED = 50.0;         // Radius squared beneath which boids repel each other
                                                    // TODO repel augmentation factor...?

    static double ALIGNMENT_FACTOR = 0.125;         // TODO units...?

    static double COHESION_FACTOR = 0.5;            // Delta-V towards the flock Centre of Mass, in 
                                                    // units per timestep

    static double TERMINAL_VELOCITY = 10.0;         // Maximum velocity vector magnitude in units per timestep 

    static double attractorX = 400.0;               // Coordinates of the attractor force core: boids too far from
    static double attractorY = 400.0;               // this point will redirect towards it.
    static double ATTRACTION_FACTOR = 0.0005;       // TODO units


    // ========== PER-BOID PROPERTIES ========== 

    Double posX;     // Components of Position Vector
    Double posY;     // (units/pixels)

    Double velX;     // Components of Velocity Vector
    Double velY;     // (units/pixels per second)


    //========== CONSTRUCTORS ========== 

    // Default constructor (probably unneeded)
    public Boid(){
        this.posX = Double.valueOf(0);
        this.posY = Double.valueOf(0);
        this.velX = Double.valueOf(0);
        this.velY = Double.valueOf(0);
    }


    // Object-Parametrized constructor
    public Boid(Double posX, Double posY, Double velX, Double velY){
        this.posX = posX;
        this.posY = posY;
        this.velX = velX;
        this.velY = velY;
    }


    // Primitive-Parametrized constructor (ah, Java, you weird little language!)
    public Boid(double posX, double posY, double velX, double velY){
        this.posX = Double.valueOf(posX);
        this.posY = Double.valueOf(posY);
        this.velX = Double.valueOf(velX);
        this.velY = Double.valueOf(velY);
    }



    // ========== SIMULATION FUNCTIONALITY========== 


    // toString function; crucial formatting for file output
    public String toString(){
        // Emit trailing comma so boids can be output together sans extra logic
        return this.posX + "," + this.posY + "," + this.velX + "," + this.velY + ",";
    }


    
    /** 
    * The meat and bread of the Boids algorithm: the boid update step.
    * Given a set of other Boids, tests them for flockmatehood (proximity) and
    * updates velocity according to the three Boids rules.
    * Updates position from velocity.
    * 
    * @param    others  The other boids to consider when performing the 
    *                   algorithm's steering rules. Each boid will be checked
    *                   for proximity; if it is closer than FLOCK_RADIUS
    *                   (see FLOCK_RADIUS_SQUARED above) it is a 'flockmate'
    *                   and this boid will steer accordingly.
    *
    * @return   nothing; invocation updates self's posX, posY, velX, and velY
    */
    public void update(Iterable<Boid> others){

        // 1) Gather flockmate influences on velocity (steering rules)
        int numActualFlockmates = 0;    // The number of actual flockmates

        double sepVecX = 0.0;           // Separation Vector: points away from other boids; length is distance to other boids.
        double sepVecY = 0.0;           // For repelling in separation rule

        double totalVelX = 0.0;         // Total velocity of flockmates (pre-division by flockmate count)
        double totalVelY = 0.0;

        double totalCOMPosX = 0.0;      // Centre of mass position (pre-division by flockmate count)
        double totalCOMPosY = 0.0;      //  "

        for(Boid neighbor : others){
            // For every potential flockmate

            double distanceSquared = Math.pow(neighbor.posX - this.posX, 2.0) + Math.pow(neighbor.posY - this.posY, 2.0);
            if(distanceSquared < FLOCK_RADIUS_SQUARED && !(neighbor.posX == this.posX && neighbor.posY == this.posY)){  // Exclude myself
                // For every actual flockmate
                numActualFlockmates++;

                // Add flockmate's contribution to separation rule
                if(distanceSquared < TOO_CLOSE_SQUARED){
                    // If too close, repel
                    sepVecX = sepVecX + (this.posX - neighbor.posX);
                    sepVecY = sepVecY + (this.posY - neighbor.posY);
                }

                // Add flockmate's contribution to alignment rule
                totalVelX = totalVelX + neighbor.velX;
                totalVelY = totalVelY + neighbor.velY;

                // Add flockmate's contribution to cohesion rule Centre-of-mass
                totalCOMPosX = totalCOMPosX + neighbor.posX;
                totalCOMPosY = totalCOMPosY + neighbor.posY;
            }
        }



        // 2) If there are flockmates to care about, apply flockmate rules to velocity
        if(numActualFlockmates > 0){

            // Add separation rule to velocity
            this.velX = this.velX + sepVecX;
            this.velY = this.velY + sepVecY;



            // Determine alignment rule average velocity
            double avgVelX = totalVelX / numActualFlockmates;
            double avgVelY = totalVelY / numActualFlockmates;

            // Add alignment rule to velocity
            this.velX = this.velX + (avgVelX - this.velX) * ALIGNMENT_FACTOR;
            this.velY = this.velY + (avgVelY - this.velY) * ALIGNMENT_FACTOR;



            // Determine cohesion rule COM
            double COMPosX = totalCOMPosX / numActualFlockmates;
            double COMPosY = totalCOMPosY / numActualFlockmates;
            double COMWardX = (COMPosX - this.posX);
            double COMWardY = (COMPosY - this.posY);

            // Normalize to get COM-ward direction
            double COMWardLength = Math.sqrt(COMWardX * COMWardX + COMWardY * COMWardY);
            COMWardX = COMWardX / COMWardLength;
            COMWardY = COMWardY / COMWardLength;
        
            // Add cohesion rule to velocity
            this.velX = this.velX + (COMWardX * COHESION_FACTOR);
            this.velY = this.velY + (COMWardY * COHESION_FACTOR);
        }


        // 3) Apply attraction force
        double attractorWardX = attractorX - this.posX;
        double attractorWardY = attractorY - this.posY;
        this.velX = this.velX + attractorWardX * ATTRACTION_FACTOR;
        this.velY = this.velY + attractorWardY * ATTRACTION_FACTOR;
        

        // 4) Clamp velocity vector to terminal velocity
        double velMag = Math.sqrt(this.velX * this.velX + this.velY * this.velY);
        if(velMag > TERMINAL_VELOCITY){
            double velShrinkFactor = TERMINAL_VELOCITY / velMag;
            this.velX = this.velX * velShrinkFactor;
            this.velY = this.velY * velShrinkFactor;
        }
        

        // 5) Update position from velocity
        this.posX = this.posX + this.velX;
        this.posY = this.posY + this.velY;
    }

}
