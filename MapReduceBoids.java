import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
//import org.apache.hadoop.mapreduce.*;

/* MapReduceBoids.java
*
*  Map Reduce-based implementation of the Boids flocking algorithm
*  written December 2022 as part of CSS 534 HW 5 by Abdul-Muizz Imtiaz
*/

public class MapReduceBoids {
    //Class for storing boid with its index i.e: 0,1,2,3,....... numofboids - 1
    public static class IndexedBoid extends Boid {
        public int boidIndex; 

        //Constructor for Boid object
        public IndexedBoid(int index, Double posX, Double posY, Double velX, Double velY) { 
            super(posX, posY, velX, velY);
            this.boidIndex = index;
        }
        
        @Override
        public String toString(){
            return this.boidIndex + "," + super.toString();
        }

    } //end of static IndexedBoids class

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text,Text> {
        int numberOfBoids = 0;
        public void configure(JobConf job) {
            numberOfBoids = Integer.parseInt(job.get("number of boids"));
        }

        public void map(LongWritable byteOffset, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            //Input key is the byte offset of each line (not used)
            //Input value is going to be 1 line in file (1 boids data for that timestep with the line number)
            //map() sends current boid's data to all other boids (including itself)

            String line = value.toString(); //calling toString() method on each line            
            String[] boidData = line.split(","); //boid split by comma
            
            //get line number (starts at line 0), position and velocity of boid 
            int index = Integer.parseInt(boidData[0]);
            Double posX = Double.valueOf(boidData[1]);
            Double posY = Double.valueOf(boidData[2]);
            Double velX = Double.valueOf(boidData[3]);
            Double velY = Double.valueOf(boidData[4]);

            //Boid currentBoid = new Boid(posX, posY, velX, velY); //create Boid object
            IndexedBoid currentIndexedBoid = new IndexedBoid(index, posX, posY, velX, velY); //create indexed boid object
            //System.out.println("Line in map: " + line); 

            //send current indexed boid data to all boids (including itself)
            for (int i = 0; i < numberOfBoids; i++) {
                output.collect(new Text(Integer.toString(i)), new Text(currentIndexedBoid.toString()));
            }          
        } //end of map method
    
    } //end of static Map class

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
            //input key is current boid index (from 0 to numBoids - 1) 
            //input values will be all Boid objects in file (not necessarily in same order as input file because of shuffle and sort stage)
            //reduce() will update current boids position and velocity and add it to output file
            
            ArrayList<Boid> boids = new ArrayList<Boid>(); //store all boid objects here
            IndexedBoid currentIndexedBoid = null; //stores current boid data so that we can update it            
            
            //read all boids and add to list
            while (values.hasNext()) {
                String line = values.next().toString(); //fetch line (represents indexed boid)
                //System.out.println("Line in reduce: " + line);
                
                String[] boidData = line.split(","); //indexed boid split by comma
                //get line number (starts at line 0), position and velocity of boid 
                int index = Integer.parseInt(boidData[0]);
                Double posX = Double.valueOf(boidData[1]);
                Double posY = Double.valueOf(boidData[2]);
                Double velX = Double.valueOf(boidData[3]);
                Double velY = Double.valueOf(boidData[4]);

                boids.add(new Boid(posX, posY, velX, velY)); //add copy of boid to boids list
                
                if (index == Integer.parseInt(key.toString())) { //if boids index is same as line number it means this is the current boid
                    currentIndexedBoid = new IndexedBoid(index, posX, posY, velX, velY);
                } 
            } //end of hasNext() loop

            currentIndexedBoid.update(boids); //update current boid                        
            String result = currentIndexedBoid.toString(); //convert boid object to string 
            output.collect(NullWritable.get(), new Text(result)); //collect result in output (key is not needed so we use NullWritable)
        
        } //end of reduce method
    } //end of static Reduce class

    //main method
    public static void main(String[] args) throws Exception {
        if (args.length != 4) { //make sure there are 4 command line args
	        System.err.println("usage: input-directory output-directory numberOfTimeSteps numberOfBoids");
	        System.exit(-1);
	    }

        //args[0] = input DIRECTORY name
        //args[1] = output DIRECTORY name
        //args[2] = timesteps (timesteps = 2 means you have initial conditions file and next timestep (t = 1) in output file (2 lines))
        //args[3] = number of boids
        //Call initialConditions file time0.txt for this to work

        int numOfTimeSteps = Integer.parseInt(args[2]); //number of timesteps in simulation 
        if (numOfTimeSteps <= 1) {
            System.err.println("numOfTimesteps has to be greater than 1");
            System.exit(-1);
        }

        long startTime = System.nanoTime(); //start timer

        FileSystem fs = null;
        //keep starting a new mapreduce job for each timestep
        for (int t = 0; t < numOfTimeSteps - 1; t++) {
            //create JobConf object for this job
            JobConf conf = new JobConf(MapReduceBoids.class);
            conf.setJobName("Boids with Map Reduce");

            //set mapper and reducer classes
            conf.setMapperClass(Map.class);
            //conf.setCombinerClass(Reduce.class);    
            conf.setReducerClass(Reduce.class);
        
            //set output key and value classes
            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);
            conf.setOutputKeyClass(NullWritable.class);
            conf.setOutputValueClass(Text.class);

            //set input and output format for job
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            //set number of boids in conf object so we can access it 
            conf.set("number of boids", args[3]);
            
            //set input and output directory names
            Path inputPath = new Path(args[0] + "/time" + t + ".txt");
            Path outputPath = new Path(args[1]);
            FileInputFormat.setInputPaths(conf, inputPath); 
            FileOutputFormat.setOutputPath(conf, outputPath); 
            
            RunningJob job = JobClient.runJob(conf); //run the job
            job.waitForCompletion();

            //job is complete so move part-00000 to input directory as time%d.txt
            Path oldPath = new Path(String.format("%s/part-00000", args[1]));
            Path newPath = new Path(String.format("%s/time%d.txt", args[0], t + 1));
            fs = FileSystem.get(conf);

            fs.rename(oldPath, newPath);
            fs.delete(new Path(args[1])); //delete output directory
        } //end of for loop for all jobs

        /*
        We are done with executing all jobs
        So we need to join all time.txt files together into one
        */
        
        //Code to copy all time.txt files to local machine 
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        for(int i = 0; i < numOfTimeSteps; i++){
            fs.copyToLocalFile(false, status[i].getPath(), new Path("/home/NETID/amimtia3/Project/"));
        }
        
        //Now they are on local machine so we combine then
        File file = new File("mapreduceoutput.txt");
        if (file.isFile() && file.exists()) {
            file.delete(); //delete any existing file with this name
        }

        FileWriter myFile = new FileWriter("mapreduceoutput.txt", true); //create output file
        PrintWriter output = new PrintWriter(myFile); // create PrintWriter object for appending to file
        
        for (int i = 0; i < numOfTimeSteps; i++) {
            Scanner input = new Scanner(new File("time" + i + ".txt")); //Scanner object to read from file
            
            //read all lines and add to output file after removing line number
            while (input.hasNext()) {
                String line = input.nextLine();
                int firstCommaIndex = line.indexOf(",");
                String s = line.substring(firstCommaIndex + 1);
                output.print(s);
            }
            output.println();
        }
        
        fs.delete(new Path(args[0])); //delete input directory in HDFS at very end
        fs.close(); //close filesystem
        output.close(); //close output stream
        
        long endTime = System.nanoTime(); //end timer
        System.out.println("Elapsed Time = " + (endTime - startTime) / 1.0e9); //print execution time in seconds

    } //end of main method

} //end of whole class

