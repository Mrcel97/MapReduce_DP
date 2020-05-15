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
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class refers to the Point 4. of the MASSIVE DATA PROCESSING, MapReduce programing practice.
 */
public class FullApplication {

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

        /* Order:
        * (M) LowerCaseMapper <Object, Text, Text, LongWritable>                            -   1 (avoid KeyError inconsistencies)
        * (M) MissingFieldsMapper <Text, LongWritable, Text, LongWritable>                  -   3 (filter the important fields)
        * (M) RedundantFieldsMapper <Text, LongWritable, Text, LongWritable>                -   2 (reduce the amount of data)
        * (M) SpanishLangMapper <Text, LongWritable, Text, LongWritable>                    -   4 (reduce the amount of data)
        * (M) TrendingTopicsMapper <Text, LongWritable, Text, LongWritable>                 -   5 (format the data)
        * (R) LongSumReducer <Text, LongWritable, Text, LongWritable>                       -   6 (join the data)
        * (I) KeyValueTextInputFormat <Text.class, Text.class, Text.class, Text.class>      -   7 (prepare TopN input data)
        * (M) TopNMapper <Text.class, LongWritable.class, NullWritable.class, Text.class>   -   8 (Apply Map)
        * (R) TopNReducer <Text.class, LongWritable.class, NullWritable.class, Text.class>  -   9 (Apply Reduce)
        *
        * Where:
        * M: Mapper
        * R: Reducer
        * I: InputFormat
        * */

        // Job
        Configuration job1Conf = new Configuration();
        tmp_config(args, job1Conf);
        output_config(args, job1Conf);
        Job job = Job.getInstance(job1Conf, "FullMaps.FullApplication");

        // MAP1: avoid KeyError inconsistencies (1)
        Configuration mapConf1_1 = new Configuration(false);
        ChainMapper.addMapper(job, LowerCaseMapper.class, Object.class, Text.class,
                Text.class, LongWritable.class,  mapConf1_1);

        // MAP2: reduce the amount of data (2)
        Configuration mapConf2 = new Configuration(false);
        ChainMapper.addMapper(job, RedundantFieldsMapper.class, Text.class, LongWritable.class,
                Text.class, LongWritable.class,  mapConf2);

        // MAP3: filter the important fields (3)
        Configuration mapConf3 = new Configuration(false);
        ChainMapper.addMapper(job, MissingFieldsMapper.class, Text.class, LongWritable.class,
                Text.class, LongWritable.class,  mapConf3);

        // MAP4: reduce the amount of data (4)
        Configuration mapConf4 = new Configuration(false);
        ChainMapper.addMapper(job, SpanishLangMapper.class, Text.class, LongWritable.class, // (4)
                Text.class, LongWritable.class, mapConf4);

        // MAP5: format the data (5)
        Configuration mapConf0 = new Configuration(false);
        ChainMapper.addMapper(job, TrendingTopicsMapper.class, Text.class, LongWritable.class, // (5)
                Text.class, LongWritable.class, mapConf0);

        // REDUCER1: join the data (6)
        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, LongSumReducer.class, Text.class, LongWritable.class, // (6)
                Text.class, LongWritable.class, reduceConf);


        job.setJarByClass(FullApplication.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Treatment
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("./tmp"));
        job.waitForCompletion(true);

        // Job 2: Map + Reduce
        Configuration job2Conf = new Configuration();
        output_config(args, job2Conf);
        job2Conf.set("n", "5");
        Job job2 = Job.getInstance(job2Conf, "MapReduce.FullApplication");
        job2.setJarByClass(FullApplication.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class); // format input (Text,Text) (7)
        job2.setMapperClass(TopNMapper.class); // MapHashtags (8)
        job2.setReducerClass(TopNReducer.class); // ReduceHashtags (9)

        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job2, new Path("./tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}