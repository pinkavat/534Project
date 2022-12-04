import sys
import tkinter

# boidsVisualizer.py
#
#   Barebones visualizer for flocking simulation output from CSS 534 Assignment 5
#
# written November 2022 by Thomas Pinkava


# Simulation timestep (milliseconds)
DELTA_T = 10

# Simulation space size (pixels / units)
SPACE_SIZE = "800x800"

# Quantum float used to ensure velocity vectors are nonzero for rendering
VERY_SMALL = 0.0000001


# Context globals for communication to/from the loop callback
# (Not worth the headache of providing a callback context)
file = None
window = None
canvas = None


def loopCallback():
    global window
    global canvas
    global file

    # Obtain the next line (timestep) of the input file
    line = file.readline()
    if not line:
        # Out of timesteps, loop back to beginning
        file.seek(0)
        line = file.readline()

    # Clear the canvas (TODO: move boids instead...?)
    canvas.delete('all')

    # Iterate over all the boids in the timestep
    floats = line.strip(',\n').split(',')
    for i in range(0, len(floats), 4):
        # Draw boid i
        boidPosX = float(floats[i])
        boidPosY = float(floats[i + 1])
        boidVelX = float(floats[i + 2])
        boidVelY = float(floats[i + 3])
        if boidVelX == 0:
            boidVelX = VERY_SMALL
        if boidVelY == 0:
            boidVelY = VERY_SMALL

        # Normalize the velocity
        velMag = boidVelX**2.0 + boidVelY**2.0
        fInvSqrt = velMag**-0.5      # Here's hoping!
        boidVelX *= fInvSqrt
        boidVelY *= fInvSqrt

        canvas.create_line(boidPosX, boidPosY, boidPosX + boidVelX, boidPosY + boidVelY, arrow='last')


    # Refresh the window
    window.update()

    # Reenqueue this function
    window.after(DELTA_T, loopCallback) 




if __name__ == "__main__":

    # Open file for reading
    try:
        file = open(sys.argv[1])
    except OSError:
        sys.exit(f"Error: Could not find/open {sys.argv[1]}")
    except:
        sys.exit("Usage: python boidsVisualizer.py <filename>")

    # Prep GUI and start loop
    window = tkinter.Tk()
    window.title("Boids Visualizer")
    window.geometry(SPACE_SIZE)

    canvas = tkinter.Canvas(window, bg = 'white')
    canvas.pack(fill="both", expand=True)

    window.after(DELTA_T, loopCallback)
    window.mainloop()

    # Close file
    try:
        file.close()
    except:
        # Huh, I guess tries need a following block. Odd.
        pass
