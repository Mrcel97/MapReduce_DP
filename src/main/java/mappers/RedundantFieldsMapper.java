package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 2.2: Removal of tweets with lacking some of the processed fields (hashtag, text)
 */
public class RedundantFieldsMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        JSONObject jObject = new JSONObject(key.toString());
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
}
