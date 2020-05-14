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
 *  - It aims to answer the Statement point 2.2: Removal of tweets with lacking some of the processed fields (hashtag, text)
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
public class RedundantFieldsMapper<K, V> extends Mapper<K, V, Text, LongWritable> implements MultiInputMapper<K, V, JSONObject> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(K key, V value, Context context) throws IOException, InterruptedException {
        JSONObject jObject = parseInput(key, value);
        JSONObject outJsonObject = new JSONObject();
        if (jObject.has("text")) {
            outJsonObject.put("text", jObject.get("text"));
        }
        if (jObject.has("entities") && jObject.getJSONObject("entities").has("hashtags")) {
            JSONObject entities = jObject.getJSONObject("entities");
            outJsonObject.put("entities", new JSONObject());
            outJsonObject.getJSONObject("entities").put("hashtags", entities.getJSONArray("hashtags"));
        }
        if (jObject.has("lang")) {
            outJsonObject.put("lang", jObject.get("lang"));
        }
        word.set(outJsonObject.toString());
        context.write(word, one);
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
