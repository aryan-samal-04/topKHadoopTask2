package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {

	private Logger logger = Logger.getLogger(TopKMapper.class);


	private PriorityQueue<WordAndCount> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println("made it into top K mapper dsldjfsalkfsaljfsalfjsflsa");
		String[] flightAndDelay = value.toString().split(",");
		if (flightAndDelay.length == 2) {
            int flightCount = Integer.parseInt(flightAndDelay[0]);  
            float totalDelay = Float.parseFloat(flightAndDelay[1]);  

            if (flightCount > 0) {  // To avoid division by zero
                pq.add(new WordAndCount(new Text(key), new FloatWritable(totalDelay / flightCount)));
			}
			if (pq.size() > 10) {
				pq.poll();
			}
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getWord(), wordAndCount.getRatio());
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}