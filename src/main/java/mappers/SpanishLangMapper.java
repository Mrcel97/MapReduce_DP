package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 2.4: Filter the tweets with a different language ("lang":"es" or "lang":"en")
 */
public class SpanishLangMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();
    public String lang = "es";

    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        JSONObject jObject = new JSONObject(key.toString());
        if (jObject.has("lang") && jObject.get("lang").equals(lang)) {
            word.set(key.toString());
            context.write(word, one);
        }
    }
}
