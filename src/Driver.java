import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Driver for MapReduce Job in Java.
 * The driver class configures and submits the job to the Hadoop cluster for execution.
 */
public class Driver extends Configured implements Tool {

    //-----------CONFIGURATION---------------------------------------------
    public static String jobName = "TTP";
    public static String inputFile = "/user/sunyi/ttp/input/facebook.txt"; //input path in hdfs
    public static String outputDir = "/user/sunyi/ttp/output/"; //output path in hdfs
    public static String localDir = "/home/sunyi/ttp/output/"; //local filesystem output directory
    public static int reducers = 1;
    public static FileSystem hdfsFileSystem;
    public static int partitions;
    //---------------------------------------------------------------------

    /**
     * Main
     * @param args class_name #partitions [#reducers] [hdfs_input_file] [hdfs_output_dir] [local_dir]
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        partitions = Integer.parseInt(args[1]);

        if (args.length > 2) {
            reducers = Integer.parseInt(args[2]);
        }

        if (args.length > 3) {
            inputFile = args[3];
        }
        if (args.length > 4) {
            outputDir = args[4];
        }
        if (args.length > 5) {
            localDir = args[5];
        }

        String dateId = dateUniqueId();
        outputDir += dateId;
        localDir += dateId;

        long startJob = System.currentTimeMillis();

        int res = ToolRunner.run(new Driver(), args);
        hdfsFileSystem.copyToLocalFile(false, new Path(outputDir + "/part-r-00000"), new Path(localDir));

        long endJob = System.currentTimeMillis();
        long jobExecTime = endJob - startJob;

        printResults(jobExecTime);

        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        hdfsFileSystem = FileSystem.get(conf);
        conf.set("partitions", args[1]);
        Job job = Job.getInstance(getConf(), jobName);
        job.setJarByClass(this.getClass());
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String dateUniqueId(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public static void printResults(long jobExecTime){
        //Read the output file to count the number of triangles
        BufferedReader reader;
        double triangles = 0;
        int linesRead = 0;
        int notOnes = 0;

        try {
            reader = new BufferedReader(new FileReader(localDir));
            String line;
            String values[];

            while ((line=reader.readLine()) != null) {
                values = line.split("\t");
                linesRead++;
                if(!values[1].equals("1.0")){
                    notOnes++;
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        double value = 1.0/(partitions-1.0);
        triangles = linesRead - notOnes + notOnes * value;

        String hms = String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(jobExecTime),
                TimeUnit.MILLISECONDS.toMinutes(jobExecTime) % TimeUnit.HOURS.toMinutes(1),
                TimeUnit.MILLISECONDS.toSeconds(jobExecTime) % TimeUnit.MINUTES.toSeconds(1));

        System.out.println("******************************************************" +
		"\nInput File = " + inputFile +
                "\nPartitions = " + partitions +
                "\nTriangles = " + triangles +
                "\nType-2 and Type-3 triangles (value 1.0) = " + (linesRead - notOnes) +
                "\nType-1 triangles (value " + value + ") = " + notOnes/value +
                "\nJob execution time = " + hms + " (" + jobExecTime + " ms)");
        System.out.println("******************************************************");
    }
}
