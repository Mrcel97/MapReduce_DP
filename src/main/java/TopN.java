import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import mappers.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {

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

    public static void tmp_config(String[] args, Configuration conf) throws IOException, URISyntaxException {
        Path outputPath = new Path("./tmp");
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);
    }

    public static void output_config(String[] args, Configuration conf) throws IOException, URISyntaxException {
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);
    }

    public static void main(String[] args) throws Exception {
        // Configuration
        System.setProperty("hadoop.home.dir", "C:\\hadoop" );

        // Job 1: Format + Count
        Configuration job1Conf = new Configuration();
        tmp_config(args, job1Conf);
        output_config(args, job1Conf);
        Job job = Job.getInstance(job1Conf, "FormatCount.Hashtags");
        job.setJarByClass(TopN.class);
        job.setMapperClass(TrendingTopicsMapper.class); // Format
        job.setReducerClass(LongSumReducer.class); // Count

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("./tmp"));
        job.waitForCompletion(true);

        // Job 2: Map + Reduce
        Configuration job2Conf = new Configuration();
        output_config(args, job2Conf);
        job2Conf.set("n", "5");
        Job job2 = Job.getInstance(job2Conf, "MapReduce.TopN");
        job2.setJarByClass(TopN.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class); // Java-provided format to parse "key\tvalue" input
        job2.setMapperClass(TopNMapper.class); // MapHashtags
        job2.setReducerClass(TopNReducer.class); // ReduceHashtags

        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job2, new Path("./tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}