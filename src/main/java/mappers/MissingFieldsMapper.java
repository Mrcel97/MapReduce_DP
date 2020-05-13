package mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 2.3: Filter the fields not used in the processing (we use only the following fields: hashtag, text and language).
 */
public class MissingFieldsMapper extends Mapper<Text, LongWritable, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        JSONObject jObject = new JSONObject(key.toString());
        if ((jObject.has("Text") || jObject.has("text")) &&
                (jObject.has("Entities") || jObject.has("entities")) &&
                jObject.getJSONObject("entities").getJSONArray("hashtags").length() >= 1) {
            word.set(key.toString().toLowerCase());
            context.write(word, one);
        }
    }
}
