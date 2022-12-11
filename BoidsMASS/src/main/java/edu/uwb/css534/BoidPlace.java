package edu.uwb.css534;

import java.util.Set;
import java.util.HashSet;
import java.util.Vector;
import edu.uw.bothell.css.dsl.MASS.*;


/* BoidPlace.java
*
*   MASS Place representation of a grid-cell subdivision of Boid simulation space
*
* written December 2022 as part of CSS 534 HW 5 by Thomas Pinkava
*/


public class BoidPlace extends Place {

        // CallAll function identifiers
        public static final int getOwnBoids_ = 1;
        public static final int exchangeBoids_ = 2;
        public static final int aggregateBoids_ = 3;

        // The set of boids currently within this grid cell;
        // updated by getOwnBoids calls from the BoidAgents currently 'here'
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


        // MASS callAll and exchangeAll handle
        public Object callMethod(int functionId, Object args){
            switch(functionId){
                case getOwnBoids_: return getOwnBoids(args);
                case exchangeBoids_: return exchangeBoids(args);
                case aggregateBoids_: return aggregateBoids(args);
            }
            return null;
        }


        // Fetches and stores boid data for all boid agents in this place
        public Set<Boid> getOwnBoids(Object args){
            selfBoids = new HashSet<Boid>();
            for(Agent boidAgent : getAgents()){
                selfBoids.add(((BoidAgent)boidAgent).boidData);
            }
            return selfBoids;
        }


        // Transmits all boids within this place
        public Set<Boid> exchangeBoids(Object args){
            return selfBoids;
        }


        // Receives all boids within this place, and adds our own boidset
        // in anticipation of agent update calls
        public Object aggregateBoids(Object args){
            Object[] messages = getInMessages();
            if(messages != null){
                for(Object boidDataObject : messages){
                    if(boidDataObject != null){ // Ignore OOB
                        selfBoids.addAll((Set<Boid>)boidDataObject);
                    }
                }
            }
            return null;
        }
}

