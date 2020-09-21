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
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Driver for MapReduce Job in Java.
 * The driver class configures and submits the job to the Hadoop cluster for execution.
 */
public class Driver extends Configured implements Tool {
    /*--- CONFIGURATION ----*/
    public static final String JOB_NAME = "TTP";
    public static final String INPUT_FILE = "/user/sunyi/ttp/input/wiki_vote.txt"; //input file path or input directory path
    public static final String OUTPUT_DIR = "/user/sunyi/ttp/output/" + dateUniqueId(); //output path
    public static final String LOCAL_DIR = "/home/sunyi/ttp/output/" + dateUniqueId();
    public static final String OUTPUT_FILE = OUTPUT_DIR + "/part-r-00000";
    public static final int NUM_REDUCE_TASK = 1;
    public static FileSystem hdfsFileSystem;
    /*----------------------*/

    /**
     * Main
     * @param args --> formato: hadoop jar DATA/analyzer.jar [#reducers] [input file/dir WET] [ input file/dir INFO] [dictionary file] [output dir]
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        System.out.println("File: " + INPUT_FILE);
        int res = ToolRunner.run(new Driver(), args); //(args: <file input>, <dir output>, <#reducer>)
        hdfsFileSystem.copyToLocalFile(false, new Path(OUTPUT_FILE), new Path(LOCAL_DIR));

        //Read the output file to count the number of triangles
        BufferedReader reader;
        double triangles = 0;
        int linesRead = 0;
        int notOnes = 0;

        try {
            reader = new BufferedReader(new FileReader(LOCAL_DIR));
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

        double value = 1/(Double.parseDouble(args[1])-1);
        triangles = linesRead - notOnes + notOnes * value;

        System.out.println("Number of triangles = " + triangles + ", lines read: " + linesRead + ", ones = " + (linesRead - notOnes) + ", not ones = " + notOnes + ", " + value);

        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        hdfsFileSystem = FileSystem.get(conf);
        conf.set("partitions", args[1]);
        Job job = Job.getInstance(getConf(), JOB_NAME);
        job.setJarByClass(this.getClass());
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(INPUT_FILE) );
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static String dateUniqueId(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
