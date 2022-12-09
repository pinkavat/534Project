package edu.uwb.css534;

import java.util.Set;
import java.util.HashSet;
import java.util.Vector;
import edu.uw.bothell.css.dsl.MASS.*;


/* BoidPlace.java
*
*   TODO doc
*
* written December 2022 as part of CSS 534 HW 5 by Thomas Pinkava
*/


public class BoidPlace extends Place {

        // CallAll function identifiers
        public static final int getOwnBoids_ = 1;
        public static final int exchangeBoids_ = 2;
        public static final int aggregateBoids_ = 3;

        // TODO doc
        Set<Boid> selfBoids;

        /**
        *   Constructor
        *
        *   Initializes a new BoidPlace with a Moore neighborhood.
        *
        *   @param args     ignored.
        */
        public BoidPlace(Object args){
            // Initialize Moore neighborhood
            Vector<int[]> neighbors = new Vector<int[]>();
            neighbors.add(new int[] {-1, -1});
            neighbors.add(new int[] {0, -1});
            neighbors.add(new int[] {1, -1});
            neighbors.add(new int[] {-1, 0});
            neighbors.add(new int[] {1, 0});
            neighbors.add(new int[] {-1, 1});
            neighbors.add(new int[] {0, 1});
            neighbors.add(new int[] {1, 1});

            setNeighbors(neighbors);
            selfBoids = new HashSet<Boid>();
        }

        // TODO doc
        public Object callMethod(int functionId, Object args){
            switch(functionId){
                case getOwnBoids_: return getOwnBoids(args);
                case exchangeBoids_: return exchangeBoids(args);
                case aggregateBoids_: return aggregateBoids(args);
            }
            return null;
        }

        // TODO doc
        public Set<Boid> getOwnBoids(Object args){
            selfBoids = new HashSet<Boid>();
            for(Agent boidAgent : getAgents()){
                selfBoids.add(((BoidAgent)boidAgent).boidData);
            }
            return selfBoids;
        }

        // TODO doc
        public Set<Boid> exchangeBoids(Object args){
            return selfBoids;
        }

        // TODO doc
        public Object aggregateBoids(Object args){
            Object[] messages = getInMessages();
            if(messages != null){
                for(Object boidDataObject : messages){
                    if(boidDataObject != null){ // TODO mention
                        selfBoids.addAll((Set<Boid>)boidDataObject);
                    }
                }
            }
            return null;
        }
}

