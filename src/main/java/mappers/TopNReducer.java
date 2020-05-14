package mappers;

import helpers.SortedTreeMap;
import helpers.TextPair;
import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 3: "Top-N pattern"<br>
 * <br>
 * Usage:<br>
 *     <b>InputKey:</b> NullWritable<br>
 *     <b>InputValue:</b> Text<br>
 *     <b>OutputKey:</b> Text<br>
 *     <b>OutputValue:</b> LongWritable<br>
 */
public class TopNReducer extends Reducer<NullWritable, Text, Text, LongWritable> {
    private final SortedTreeMap<LongWritable, Text> customTopN = new SortedTreeMap<>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int max_records = Integer.parseInt(context.getConfiguration().get("n"));

        for (Text value : values) {
            TextPair<Text, LongWritable> entry = TextPair.fromText(value); // format is "Text,LongWritable"

            if (customTopN.size() < max_records) {
                customTopN.append(entry.getValue(), entry.getKey()); // swap (key, value) so Key sorting takes effect
            }
        }

        List<LongWritable> keys = customTopN.keys;
        Comparator<LongWritable> comparator = Comparator.comparingLong(LongWritable::get);
        keys.sort(comparator.reversed());
        for (LongWritable mapKey : keys) {
            Pair<LongWritable, Text> item = customTopN.extract(mapKey);
            if (item == null) return;
            context.write(item.getValue(), item.getKey());
        }
    }
}