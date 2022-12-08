import sys
import math
import random

# boidsInitialConditionGeneratorFORMAPREDUCE.py
#
#   Generates a field of Boids (float 4-tuples) as specified in
#   the HW5 project (see Boid.java, perhaps) to use as a starting
#   state for the flocking simulation
#   
#   FOR MAPREDUCE: PREPENDS AN INTEGER DEFINING THE BOID'S INDEX.
#   This is to circumvent the MapReduce shuffling.
#
# written November 2022 by Thomas Pinkava

# Command-line Parameters:
#
#   1) Space Width
#   2) Space Height
#   3) Offset X (will be added to all Boids)
#   4) Offset Y ( " )
#   5) Number of Boids to place (APPROXIMATE)
#   6) Initial Boid Velocity-Vector length (vectors will point in random directions)
#
#   Pipe stdout to a file, then feed said file into the flocking simulators.
#
if __name__ == "__main__":
    if(len(sys.argv) < 7):
        sys.exit("Usage: python boidsInitialConditionGenerator.py <space width> <space height> <offset x> <offset y> <number of boids> <initial vel magnitude>");

    # Work out what size of perturbed-placement grid we're in for
    sidelen = int(math.sqrt(float(sys.argv[5])))

    # Work out dimensions
    gridX = float(sys.argv[1]) / float(sidelen)
    gridY = float(sys.argv[2]) / float(sidelen)

    offsetX = float(sys.argv[3])
    offsetY = float(sys.argv[4])
    velMag = float(sys.argv[6])

    
    # Place perturbed boids
    index = 0
    for y in range(sidelen):
        for x in range(sidelen):
            posX = offsetX + (float(x) * gridX) + (random.random() * gridX)
            posY = offsetY + (float(y) * gridY) + (random.random() * gridY)
            randAngle = random.random() * math.tau
            velX = math.cos(randAngle) * velMag
            velY = math.sin(randAngle) * velMag
            print(index,posX,posY,velX,velY,'', sep=',')
            index = index + 1
