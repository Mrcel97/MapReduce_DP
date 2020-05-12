import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// This code will make use of snake_case as a prove of my nostalgia

public class WordCount {

//    public static class TokenizerMapper - No longer needed, now we will use regex as our Mapper

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

    public static String[] mapper_config(String[] args, Configuration conf) throws IOException {
        conf.set("mapreduce.mapper.regex", "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
        return new GenericOptionsParser(conf, args).getRemainingArgs();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop" );
        Configuration conf = new Configuration();
        output_config(args, conf);
        String[] parsedArgs = mapper_config(args, conf);
        Job job = Job.getInstance(conf, "TrendingTopic.HashtagCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(RegexMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(parsedArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(parsedArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}