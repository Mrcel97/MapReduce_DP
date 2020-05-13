package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 2.1: Change the tweets letter case to either lower-case
 */
public class LowerCaseMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        word.set(value.toString().toLowerCase());
        context.write(word, one);
    }
}