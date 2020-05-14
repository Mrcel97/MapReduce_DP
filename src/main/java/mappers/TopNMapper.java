package mappers;

import helpers.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNMapper extends Mapper<Text, LongWritable, NullWritable, Text> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    private TreeMap<LongWritable, TextPair> topN = new TreeMap<>();

    public void map(Text key, LongWritable value, Context context) {
        int max_registers = Integer.parseInt(context.getConfiguration().get("n"));

        if (topN.size() <= 2 * max_registers) {
            topN.put(value, new TextPair(key, value));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<LongWritable, TextPair> entry : topN.entrySet()) {
            context.write(NullWritable.get(), entry.getValue().toText());
        }
    }
}
