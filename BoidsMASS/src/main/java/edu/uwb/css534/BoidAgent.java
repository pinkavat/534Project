package edu.uwb.css534;

import java.util.Set;
import edu.uw.bothell.css.dsl.MASS.Agent;

/* BoidAgent.java
*
*   TODO doc
*
* written December 2022 as part of CSS 534 HW 5 by Thomas Pinkava
*/


public class BoidAgent extends Agent {

        // CallAll function identifiers
        public static final int init_ = 1;
        public static final int updateBoid_ = 2;

        // Boid data associated with this agent
        public Boid boidData;

        // TODO doc
        public BoidAgent(Object args){
            boidData = (Boid)args;
        }

        // TODO doc
        public Object callMethod(int functionId, Object args){
            switch(functionId){
                case init_: return init(args);
                case updateBoid_ : return updateBoid(args);
            }
            return null;
        }

        // TODO doc
        public Object init(Object args){
            boidData = (Boid)args;
            return null;
        }

        // TODO doc
        public Object updateBoid(Object args){
            // Fetch neighborhood Boid Data from place
            Set<Boid> neighbors = ((BoidPlace)getPlace()).selfBoids;

            // Update self with neighbor data
            boidData.update(neighbors);

            // TODO migrate if necessary
            return null;
        }
}
