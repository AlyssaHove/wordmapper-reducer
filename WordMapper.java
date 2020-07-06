//Alyssa Hove and Katheryn Weeden
//4/26
// remap the result
// <Word, num of occur> -> list of word, <1>
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String s = value.toString();
        for (String word : s.split("\\W+").toLowerCase()) { // split it to lower case
            if (word.length() > 0 && !this.isNumeric(word)) {
                    context.write(new Text(word.toLowerCase()), new IntWritable(1));
                }
    }
}//map class

    public boolean isNumeric(String s) { // check if string is numeric
        return s.matches("[-+]?\\d*\\.?\\d+");
    }// is Numeric
}//Mapper
