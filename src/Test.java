import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class Test {
    public static void main(String[] args) {
        String[] t = {"1,2", "1,3", "1,4","1,6","1,7","2,3","2,5","2,10","2,11","3,4","3,5","3,8","4,8","5,9","6,7","9,12"};
        String[] t1 = {"0,2", "0,4", "1,3","1,4","2,4","3,4","4,5"};
        String[] t2 = {"17095,89796", "26130,89796", "26130,17095"};
        //Graph g = new Graph(t2);
        //HashSet<int[]> result = g.compactForward();

        /*HashSet<int[]> result = g.compactForward();

        for(int[] r: result){
            System.out.println("triangolo:" + r[0] + r[1] + r[2]);
        }*/

        System.out.println(62512 % 201);
        System.out.println(23517 % 201);
        System.out.println(92360 % 201);
        BufferedReader reader;

        try {
            reader = new BufferedReader(new FileReader("/home/sunyi/Desktop/Progetto_bigdata/input/condense_matter.txt"));
            FileWriter myWriter = new FileWriter("/home/sunyi/Desktop/Progetto_bigdata/input/condense_matter2.txt");
            String line;
            int linesRead = 0;

            while ((line=reader.readLine()) != null && linesRead < 2000) {
                linesRead++;
                myWriter.write(line + "\n");

            }
            reader.close();
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
