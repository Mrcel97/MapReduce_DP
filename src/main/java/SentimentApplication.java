import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import mappers.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class refers to the Point 4. of the MASSIVE DATA PROCESSING, MapReduce programing practice.
 */
public class SentimentApplication {

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
        System.setProperty("hadoop.home.dir", "C:\\hadoop" );

        Configuration job1Conf = new Configuration();
        output_config(args, job1Conf);
        job1Conf.set("lexicon", args[2]);
        job1Conf.set("sentiment", args[3]);
        Job job = Job.getInstance(job1Conf, "Sentiment.Computation");
        job.setJarByClass(SentimentApplication.class);
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}