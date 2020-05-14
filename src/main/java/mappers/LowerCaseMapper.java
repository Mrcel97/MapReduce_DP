package mappers;

import helpers.MultiInputMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InvalidClassException;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 2.1: Change the tweets letter case to either lower-case<br>
 * <br>
 * Usage:<br>
 *     <b>InputKey:</b> Object or Text<br>
 *     <b>InputValue:</b> Text or NullWritable<br>
 *     <b>OutputKey:</b> Text<br>
 *     <b>OutputValue:</b> LongWritable<br>
 * <br>
 * Warning:<br>
 *     Types from InputKey and InputValue go together the only combinations available are:<br>
 *         - Object and Text<br>
 *         - Text and NullWritable<br>
 */
public class LowerCaseMapper<K, V> extends Mapper<K, V, Text, LongWritable> implements MultiInputMapper<K, V, String> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(K key, V value, Context context) throws IOException, InterruptedException {
        word.set(value.toString().toLowerCase());
        context.write(word, one);
    }

    @Override
    public String parseInput(K key, V value) throws InvalidClassException {
        if (value instanceof Text) {
            return value.toString();
        } else if (key instanceof Text) {
            return key.toString();
        } else {
            throw new InvalidClassException("This Class only accepts 2 input data formats:\n" +
                    "  - Object, Text" +
                    "  - Text, LongWritable");
        }
    }
}