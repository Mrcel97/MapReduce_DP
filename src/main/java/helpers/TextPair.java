package helpers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TextPair extends Text {
    public Text first;
    public LongWritable second;

    public TextPair(Text first, LongWritable second) {
        this.first = first;
        this.second = second;
    }

    public Text first() {
        return this.first;
    }

    public LongWritable second() {
        return this.second;
    }

    public static TextPair fromText(Text text) {
        String[] pair = text.toString().split(",");
        Text first = new Text(pair[0]);
        LongWritable second = new LongWritable(Long.parseLong(pair[1]));
        return new TextPair(first, second);
    }

    public Text toText() {
        return new Text(this.first.toString() + "," + this.second.toString());
    }
}
