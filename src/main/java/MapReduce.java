import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import mappers.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class refers to the Point 2. of the MASSIVE DATA PROCESSING, MapReduce programing practice.
 */
public class MapReduce {

    public static class LongSumReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void output_config(String[] args, Configuration conf) throws IOException, URISyntaxException {
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);
    }

    public static void main(String[] args) throws Exception {
        // Configuration
        System.setProperty("hadoop.home.dir", "C:\\hadoop" );
        Configuration conf = new Configuration();
        output_config(args, conf);

        // Job
        Job job = Job.getInstance(conf, "TrendingTopic.HashtagCount");

        // MAP1(linked_version): Tweet to lowercase
        Configuration mapConf1_1 = new Configuration(false);
        ChainMapper.addMapper(job, LowerCaseMapper.class, Object.class, Text.class,
                Text.class, LongWritable.class,  mapConf1_1);

        // MAP2: Clean missing data (text, hashtags)
        Configuration mapConf2 = new Configuration(false);
        ChainMapper.addMapper(job, MissingFieldsMapper.class, Text.class, LongWritable.class,
                Text.class, LongWritable.class,  mapConf2);

        // MAP3: Clean missing data (text, hashtags)
        Configuration mapConf3 = new Configuration(false);
        ChainMapper.addMapper(job, RedundantFieldsMapper.class, Text.class, LongWritable.class,
                Text.class, LongWritable.class,  mapConf3);

        // MAP4: Filter by specific language
        Configuration mapConf4 = new Configuration(false);
        ChainMapper.addMapper(job, SpanishLangMapper.class, Text.class, LongWritable.class,
                Text.class, LongWritable.class, mapConf4);

        job.setJarByClass(MapReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Treatment
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}