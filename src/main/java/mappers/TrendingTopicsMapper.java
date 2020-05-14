package mappers;

import helpers.MultiInputMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InvalidClassException;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 4: "Join all MapReduce jobs".<br>
 * <br>
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
public class TrendingTopicsMapper<K, V> extends Mapper<K, V, Text, LongWritable> implements MultiInputMapper<K, V, JSONObject> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(K key, V value, Context context) throws IOException, InterruptedException {
        JSONObject jObject = parseInput(key, value);
        if (jObject.has("entities")) {
            JSONObject entities = jObject.getJSONObject("entities");
            if (entities.has("hashtags")) {
                JSONArray hashtags = entities.getJSONArray("hashtags");
                for (Object item : hashtags) {
                    JSONObject hashtag = (JSONObject) item;
                    word.set(hashtag.get("text").toString());
                    context.write(word, one);
                }
            }
        }
    }

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
