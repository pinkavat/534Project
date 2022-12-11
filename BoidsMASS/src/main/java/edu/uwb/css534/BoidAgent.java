package edu.uwb.css534;

import java.util.Set;
import edu.uw.bothell.css.dsl.MASS.Agent;

/* BoidAgent.java
*
*   MASS Agent wrapper for the Boid object
*
* written December 2022 as part of CSS 534 HW 5 by Thomas Pinkava
*/


public class BoidAgent extends Agent {

        // CallAll function identifiers
        public static final int init_ = 1;
        public static final int updateBoid_ = 2;
        public static final int moveBoid_ = 3;

        // Boid data associated with this agent
        public Boid boidData;

        // Spatial hashing grid data, passed in upon construction
        int gridX;
        int gridY;
        double cellX;
        double cellY;


        // Constructor: sets the grid dimensions
        public BoidAgent(Object args){
            double[] dimensions = (double[])args;
            gridX = (int)dimensions[0];
            gridY = (int)dimensions[1];
            cellX = dimensions[2];
            cellY = dimensions[3];
        }


        // MASS library handle for callAll calls
        public Object callMethod(int functionId, Object args){
            switch(functionId){
                case init_: return init(args);
                case updateBoid_ : return updateBoid(args);
                case moveBoid_ : return moveBoid(args);
            }
            return null;
        }


        // Initialization function on per-agent basis: sets Boid data
        public Object init(Object args){
            boidData = (Boid)args;
            return null;
        }


        // Update function: calls the update function of the Boid object
        // for which this Agent is a wrapper
        public Object updateBoid(Object args){
            // Fetch neighborhood Boid Data from place
            Set<Boid> neighbors = ((BoidPlace)getPlace()).selfBoids;

            // Update self with neighbor data
            boidData.update(neighbors);

            return null;
        }

        
        // Move function: invoked whenever the Boid object might have
        // switched grid cells; will move the Agent to the appropriate
        // Place, if so.
        public Object moveBoid(Object args){

            // Establish the new grid square from the Boid's position
            int targetSquareX = (int)(boidData.posX / cellX);
            int targetSquareY = (int)(boidData.posY / cellY);

            // Clamp to grid
            if(targetSquareX < 0) targetSquareX = 0;
            if(targetSquareY < 0) targetSquareY = 0;
            if(targetSquareX >= gridX) targetSquareX = gridX - 1;
            if(targetSquareY >= gridY) targetSquareY = gridY - 1;

            // Migrate to new grid square
            migrate(targetSquareX, targetSquareY);
            
            return null;
        }
}
