package mappers;

import helpers.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private TreeMap<LongWritable, Text> topN = new TreeMap<>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int max_registers = Integer.parseInt(context.getConfiguration().get("n"));

        for (Text value : values) {
            TextPair entry = TextPair.fromText(value);

            topN.put(entry.second, entry.first);

            if (topN.size() <= max_registers) {
                topN.remove(topN.firstKey());
            }
        }

        for (Text word : topN.descendingMap().values()) {
            context.write(NullWritable.get(), word);
        }
    }
}