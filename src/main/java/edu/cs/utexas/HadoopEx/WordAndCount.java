package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class WordAndCount implements Comparable<WordAndCount> {

    private Text word; 
    private FloatWritable ratio;  

    public WordAndCount() {
        this.word = new Text();
        this.ratio = new FloatWritable();
    }

    public WordAndCount(Text word, FloatWritable ratio) {
        this.word = word;
        this.ratio = ratio;
    }

    public Text getWord() {
        return word;
    }

    public FloatWritable getRatio() {
        return ratio;
    }


    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
    @Override
    public int compareTo(WordAndCount other) {

        float diff = this.ratio.get() - other.ratio.get();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }
        return 0;
    } 


    public String toString() {
        return "(" + word.toString() + " , Ratio: " + ratio.get() + ")";
    }
}
