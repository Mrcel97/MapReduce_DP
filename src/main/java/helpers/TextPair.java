package helpers;

import javafx.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TextPair<K, V> extends Pair<K, V> {

    public TextPair(K first, V second) {
        super(first, second);
    }

    public static TextPair<Text, LongWritable> fromText(Text text) {
        String[] pair = text.toString().split(",");
        Text first = new Text(pair[0]);
        LongWritable second = new LongWritable(Long.parseLong(pair[1]));
        return new TextPair<>(first, second);
    }

    public Text toText() {
        return new Text(this.getValue() + "," + this.getKey());
    }
}
