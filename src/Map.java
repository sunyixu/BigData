import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text key = new Text();
    private Text value = new Text();


    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
        String[] line = lineText.toString().split("\t");
        int p = Integer.parseInt(context.getConfiguration().get("partitions"));
        //todo cambiare stringtokenizer con il metodo split di string
        int node1 = Integer.parseInt(line[0]);
        int node2 = Integer.parseInt(line[1]);
        int part1 = node1 % p;
        int part2 = node2 % p;

        if(part1 > part2){
            part2 = part1;
            part1 = node2 % p;
        }

        Text key = new Text();
        Text value = new Text();

        //todo implementare con tuple?
        for(int a = 0; a <= p-2; a++){
            for(int b = a + 1; b <= p - 1; b++){
                if ((part1 == a && part2 == b) || (part1 == part2 && (part1 == a || part1 == b))){
                    key.set(a + "," + b);
                    value.set(node1 + "," + node2);
                    context.write(key,value);
                }
            }
        }

        if (part1 != part2) {
            for (int a = 0; a <= p - 3; a++) {
                for (int b = a + 1; b <= p - 2; b++) {
                    for (int c = b + 1; c <= p - 1; c++) {
                        if ((part1 == a || part1 == b || part1 == c) && (part2 == a || part2 == b || part2 == c)) {
                            key.set(a + "," + b + "," + c);
                            value.set(node1 + "," + node2);
                            context.write(key, value);
                        }
                    }
                }
            }
        }


    }
}