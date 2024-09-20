package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
       int flight_sum = 0;
       float delay_sum = 0;
       System.out.println("here is the reducer in wordcountreducer");
       for (Text value : values) {
            String[] tmp = value.toString().split(",");
            if (tmp.length == 2) { 
                try {
                    flight_sum += Integer.parseInt(tmp[1]);
                    delay_sum += Float.parseFloat(tmp[0]);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing value: " + value.toString());
                }
            }
        }
       context.write(text, new Text(Integer.toString(flight_sum) + "," + Float.toString(delay_sum)));
   }
}