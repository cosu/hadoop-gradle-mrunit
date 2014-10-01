import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import ro.cosu.wordcount.WordMapper;
import ro.cosu.wordcount.WordReducer;

import java.util.ArrayList;
import java.util.List;

/**
 * User: Cosmin 'cosu' Dumitru - cosu@cosu.ro
 * Date: 01/10/14
 * Time: 10:19
 */
public class WordCountTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordMapper mapper = new WordMapper();
        WordReducer reducer = new WordReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }
    @Test
    public void testMapper() throws Exception{
        mapDriver.withInput(new LongWritable(), new Text(
                "test"));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception{
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("test"), values);
        reduceDriver.withOutput(new Text("test"), new IntWritable(2));
        reduceDriver.runTest();
    }

}
