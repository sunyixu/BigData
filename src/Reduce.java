import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.HashSet;

public class Reduce extends Reducer<Text, Text, Text, FloatWritable> {

    @Override
    public void reduce(Text partitions, Iterable<Text> edges, Context context) throws IOException, InterruptedException {
        Text key = new Text();
        FloatWritable value = new FloatWritable(1);
        float p = Float.parseFloat(context.getConfiguration().get("partitions"));

        Graph g = new Graph(edges);
        HashSet<int[]> triangles = g.compactForward();

        for(int[] t: triangles){
            key.set("(" + t[0] + "," + t[1] + "," + t[2] + ")");
            if(t[0] % p == t[1] % p &&  t[1] % p == t[2] % p){
                value.set(1/(p-1));
            }else{
                value.set(1);
            }
            context.write(key, value);
        }
    }
}
