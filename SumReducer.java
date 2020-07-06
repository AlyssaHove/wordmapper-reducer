//  Alyssa Hove
// Edit reducer to put the 10 most occuring words
//mapper put the numeber of occurances as the key and the text as the value
// list of word count pairs to list of k most frequent counts
// Works only with full text

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

private static final TreeMap <IntWritable, Text> TopWords = new TreeMap <IntWritable, Text>();

        @Override
        public void reduce (Text key, Iterable<IntWritable> values, Context context)
                throws IOException,InterruptedException {

                int wordCount = 0;
                for (IntWritable value : values) {
                        wordCount += value.get();
                }
                TopWords.put(new IntWritable(wordCount),new Text(key));
                if(TopWords.size() > 10){
                TopWords.remove(TopWords.firstKey()); // remove top (remove least occuring word)
                }
}// reduce

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        for (IntWritable k : TopWords.keySet()) {
                context.write(TopWords.get(k),k);
        }
} // cleanup
}// sum reducer

