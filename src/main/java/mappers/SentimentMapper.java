package mappers;

import helpers.Lexicon;
import helpers.MultiInputMapper;
import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InvalidClassException;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 5: "Sentiment of hashtags"<br>
 * <br>
 * Usage:<br>
 *     <b>InputKey:</b> Object or Text<br>
 *     <b>InputValue:</b> Text or LongWritable<br>
 *     <b>OutputKey:</b> NullWritable<br>
 *     <b>OutputValue:</b> Text<br>
 * <br>
 * Warning:<br>
 *     Types from InputKey and InputValue go together the only combinations available are:<br>
 *         - Object and Text<br>
 *         - Text and LongWritable<br>
 */
public class SentimentMapper<K, V> extends Mapper<K, V, NullWritable, Text> implements MultiInputMapper<K, V, JSONObject> {
    private Lexicon lexicon;
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public SentimentMapper() { }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.lexicon = new Lexicon(context.getConfiguration().get("lexicon"));
    }

    public void map(K key, V value, Context context) throws IOException, InterruptedException {
        JSONObject jObject = parseInput(key, value);

        if (!jObject.has("text") || !jObject.has("lang")) return;

        String message = jObject.get("text").toString();
        Pair<Long, Long> scoreData = lexicon.sentimentComputation(jObject.get("lang").toString(), message);
        jObject.put("lexicon", scoreData.getKey());
        jObject.put("textLen", scoreData.getValue());
        word.set(jObject.toString());
        context.write(NullWritable.get(), word);
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
