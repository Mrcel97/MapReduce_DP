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
 *  - It aims to answer the Statement point 2.3: Filter the fields not used in the processing (we use only the following fields: hashtag, text and language).
 *  <br>
 * Usage:<br>
 *     <b>InputKey:</b> Text or Object<br>
 *     <b>InputValue:</b> LongWritable or Text<br>
 *     <b>OutputKey:</b> Text<br>
 *     <b>OutputValue:</b> LongWritable<br>
 * <br>
 * Warning:<br>
 *     Types from InputKey and InputValue go together the only combinations available are:<br>
 *         - Text and LongWritable<br>
 *         - Object and Text<br>
 */
public class MissingFieldsMapper<K, V> extends Mapper<K, V, Text, LongWritable> implements MultiInputMapper<K, V, JSONObject> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(K key, V value, Context context) throws IOException, InterruptedException {
        JSONObject jObject = parseInput(key, value);
        if ((jObject.has("Text") || jObject.has("text")) &&
                (jObject.has("Entities") || jObject.has("entities")) &&
                jObject.getJSONObject("entities").getJSONArray("hashtags").length() >= 1) {
            word.set(key.toString().toLowerCase());
            context.write(word, one);
        }
    }

    @Override
    public JSONObject parseInput(K key, V value) throws InvalidClassException {
        if (key instanceof Text) {
            return new JSONObject(key.toString());
        } else if (value instanceof Text) {
            return new JSONObject(value.toString());
        } else {
            throw new InvalidClassException("This Class only accepts 2 input data formats:\n" +
                    "  - Text, LongWritable" +
                    "  - Object, Text");
        }
    }
}
