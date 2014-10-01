package ro.cosu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


/**
 * User: Cosmin 'cosu' Dumitru - cosu@cosu.ro
 * Date: 01/10/14
 * Time: 10:33
 */
public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        System.out.print("Got the following values for key " + key.toString() + ":");

        for ( IntWritable value: values ) {
            sum = sum + value.get();
        }

        context.write(key, new IntWritable(sum));

    }
}
