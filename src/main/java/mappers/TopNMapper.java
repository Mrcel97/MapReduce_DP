package mappers;

import helpers.SortedTreeMap;
import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InvalidClassException;
import java.util.List;

/**
 * This Mapper is part of the MASSIVE DATA PROCESSING, MapReduce programing practice.<br>
 *  - It aims to answer the Statement point 3: "Top-N pattern"<br>
 * <br>
 * Usage:<br>
 *     <b>InputKey:</b> Text<br>
 *     <b>InputValue:</b> Text or LongWritable<br>
 *     <b>OutputKey:</b> NullWritable<br>
 *     <b>OutputValue:</b> Text<br>
 */
public class TopNMapper<K extends Text, V> extends Mapper<K, V, NullWritable, Text> {
    private final static LongWritable one = new LongWritable(1);
    private final Text word = new Text();

    private final SortedTreeMap<LongWritable, Pair<LongWritable, Text>> customTopN = new SortedTreeMap<>();

    public void map(K key, V value, Context context) throws InvalidClassException {
        int max_records = Integer.parseInt(context.getConfiguration().get("n"));
        LongWritable numItems = parseValue(value);
        if (customTopN.size() <= 2 * max_records) {
            Pair<LongWritable, Text> insertion = new Pair<>(numItems, new Text(key)); // (deep-copy) to break reference
            customTopN.append(numItems, insertion);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<LongWritable> sortedKeys = customTopN.getSortedKeys();
        for (LongWritable key : sortedKeys) {
            Pair<LongWritable, Pair<LongWritable, Text>> item = customTopN.extract(key);
            if (item == null) return;
            context.write(NullWritable.get(), new Text(item.getValue().getValue() + "," + item.getKey()));
        }
    }

    private LongWritable parseValue(V value) throws InvalidClassException {
        if (value instanceof Text) {
            return new LongWritable(Long.parseLong(value.toString()));
        } else if (value instanceof LongWritable) {
            return new LongWritable(((LongWritable) value).get());
        } else {
            throw new InvalidClassException("This Class only accepts 2 formats as InputValue:\n" +
                    "  - Text" +
                    "  - LongWritable");
        }
    }
}
