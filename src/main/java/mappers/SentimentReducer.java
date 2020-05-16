package mappers;

import javafx.util.Pair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

public class SentimentReducer extends Reducer<NullWritable, Text, Text, DoubleWritable> {
    private final Map<String, Pair<Double, Double>> sentimentCollector = new HashMap<>(); // Hashtag-based

    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean hashtag_based = context.getConfiguration().get("sentiment").equals("hashtag");

        if (!hashtag_based) {
            tweetBased(key, values, context);
            return;
        }

        for (Text value : values) {
            JSONObject jObject = new JSONObject(value.toString());

            if (this.validJsonObject(jObject)) {
                JSONObject entities = jObject.getJSONObject("entities");
                JSONArray hashtags = entities.getJSONArray("hashtags");
                for (Object item : hashtags) {
                    JSONObject hashtagEntity = (JSONObject) item;
                    double score = Long.parseLong(jObject.get("lexicon").toString());
                    double textLen = Long.parseLong(jObject.get("textLen").toString());
                    String hashtag = hashtagEntity.get("text").toString();
                    if (sentimentCollector.containsKey(hashtag)) {
                        sentimentCollector.put(hashtag, new Pair<>(sentimentCollector.get(hashtag).getKey() + score,
                                textLen));
                    } else {
                        sentimentCollector.put(hashtag, new Pair<>(score, textLen));
                    }
                }
            }
        }
        for (Map.Entry<String, Pair<Double, Double>> entry : sentimentCollector.entrySet()) {
            double lexiconScore = entry.getValue().getKey();
            double tweetLen = entry.getValue().getValue(); // in words as lexicon knows positive/negative based on words
            if (tweetLen <= -1) { return; }
            context.write(new Text(entry.getKey()), new DoubleWritable(lexiconScore/tweetLen));
        }
    }

    private void tweetBased(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            JSONObject jObject = new JSONObject(value.toString());
            if (!this.validJsonObject(jObject)) { return; }
            double score = Long.parseLong(jObject.get("lexicon").toString());
            double textLen = Long.parseLong(jObject.get("textLen").toString());
            context.write(value, new DoubleWritable(score/textLen));
        }
    }

    private boolean validJsonObject(JSONObject jObject) {
        return jObject.has("text") && jObject.get("text").toString().length() >= 1 &&
                jObject.has("entities") && jObject.getJSONObject("entities").has("hashtags") &&
                jObject.getJSONObject("entities").getJSONArray("hashtags").length() >= 1 &&
                jObject.has("lexicon") && jObject.has("textLen");
    }
}
