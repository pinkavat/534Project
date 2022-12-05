import java.io.Serializable;

/* Boid.java
*
*   Class for a single "Boid" (flocking particle) object.
*   Since all three of our parallelization programs use Java, this
*   is an obvious simplification.
*
* written November 2022 as part of CSS 534 HW 5 by Thomas Pinkava and Abdul-Muizz Imtiaz
*/

public class Boid implements Serializable {
    // Serializable, to be pushed down network pipes by the 
    // parallelization libraries


    // ========== SIMULATION PARAMETERS (shared between all boids) ==========

    static double FLOCK_RADIUS_SQUARED = 10000.0;   // Radius squared of local flock in units squared

    static double TOO_CLOSE_SQUARED = 70.0;         // Radius squared beneath which boids repel each other

    static double ALIGNMENT_FACTOR = 0.325;         // Contribution of alignment rule conditioned by this factor

    static double COHESION_FACTOR = 0.01;           // Contribution of cohesion rule conditioned by this factor

    static double TERMINAL_VELOCITY = 10.0;         // Maximum velocity vector magnitude in units per timestep 

    static double attractorX = 400.0;               // Coordinates of the attractor force core: boids too far from
    static double attractorY = 400.0;               // this point will redirect towards it.
    static double ATTRACTION_FACTOR = 0.001;        // Effect of attractor conditioned by this factor



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
    @Override
    public String toString(){
        // Emit trailing comma so boids can be output together sans extra logic
        return this.posX + "," + this.posY + "," + this.velX + "," + this.velY + ",";
    }


    // Equality function: necessary for Spark approach
    @Override
    public boolean equals(Object other){
        if (other instanceof Boid){

            Boid otherBoid = (Boid)other;

            // Compare by position (good enough)
            if(this.posX == otherBoid.posX && this.posY == otherBoid.posY) return true;
        }
        return false;
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
        int numActualFlockmates = 0;

        double sepContribX = 0.0, sepContribY = 0.0;    // Contribution to velocity from separation rule
        double velTotalX = 0.0, velTotalY = 0.0;        // Contribution to velocity from alignment rule
        double COMTotalX = 0.0, COMTotalY = 0.0;        // Contribution to velocity from cohesion rule

        for( Boid neighbor : others){
            // For every potential flockmate

            double distanceSquared = Math.pow(neighbor.posX - this.posX, 2.0) + Math.pow(neighbor.posY - this.posY, 2.0);
            if(distanceSquared < FLOCK_RADIUS_SQUARED && !(this.equals(neighbor))){ // Exclude self
                // For every actual flockmate

                numActualFlockmates++;

                // If too close, add flockmate's contribution to separation rule
                if(distanceSquared < TOO_CLOSE_SQUARED){
                    sepContribX += this.posX - neighbor.posX;
                    sepContribY += this.posY - neighbor.posY;
                }

                // Add flockmate's contribution to alignment rule
                velTotalX += neighbor.velX;
                velTotalY += neighbor.velY;
    
                // Add flockmate's contribution to cohesion rule
                COMTotalX += neighbor.posX - this.posX;
                COMTotalY += neighbor.posY - this.posY;
            }
        }


        // 2) If there are flockmates to care about, apply rules to velocity
        if(numActualFlockmates > 0){

            //Divide total contributions by number of flockmates
            double velContribX = ((velTotalX / (double)numActualFlockmates)) * ALIGNMENT_FACTOR;
            double velContribY = ((velTotalY / (double)numActualFlockmates)) * ALIGNMENT_FACTOR;

            double COMContribX = ((COMTotalX / (double)numActualFlockmates)) * COHESION_FACTOR;
            double COMContribY = ((COMTotalY / (double)numActualFlockmates)) * COHESION_FACTOR;

            // Add to velocity vector
            this.velX += sepContribX + velContribX + COMContribX;
            this.velY += sepContribY + velContribY + COMContribY;
        }

     
        // 3) Apply attraction force (to keep boids on screen)
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
