package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 1: "Trending topics"
 */
public class TrendingTopicsMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        JSONObject jObject = new JSONObject(key.toString());
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
}
