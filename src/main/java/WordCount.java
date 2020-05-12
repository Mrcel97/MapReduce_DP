import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONObject;

public class WordCount {

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

    public static class TrendingTopicsMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jObject = new JSONObject(value.toString());
            if (jObject.has("entities")) {
                JSONObject entities = jObject.getJSONObject("entities");
                if (entities.has("hashtags")) {
                    JSONArray hashtags = entities.getJSONArray("hashtags");
                    for (Object item : hashtags) {
                        JSONObject hashtag = (JSONObject) item;
                        word.set(hashtag.get("text").toString());
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class LowerCaseMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            word.set(value.toString().toLowerCase());
            context.write(word, one);
        }
    }

    public static class MissingFieldsMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jObject = new JSONObject(value.toString());
            if ((jObject.has("Text") || jObject.has("text")) &&
                    (jObject.has("Entities") || jObject.has("entities")) &&
                    jObject.getJSONObject("entities").getJSONArray("hashtags").length() >= 1) {
                word.set(value.toString().toLowerCase());
                context.write(word, one);
            }
        }
    }

    public static class RedundantFieldsMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jObject = new JSONObject(value.toString());
            JSONObject outJsonObject = new JSONObject();
            if (jObject.has("text")) {
                outJsonObject.put("text", jObject.get("text"));
            }
            if (jObject.has("entities") && jObject.getJSONObject("entities").has("hashtags")) {
                JSONObject entities = jObject.getJSONObject("entities");
                outJsonObject.put("entities", new JSONObject());
                outJsonObject.getJSONObject("entities").put("hashtags", entities.getJSONArray("hashtags"));
            }
            if (jObject.has("lang")) {
                outJsonObject.put("lang", jObject.get("lang"));
            }
            word.set(outJsonObject.toString());
            context.write(word, one);
        }
    }

    public static class SpecificLangMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();
        public String lang = "es";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jObject = new JSONObject(value.toString());
            if (jObject.has("lang") && jObject.get("lang").equals(lang)) {
                word.set(value.toString());
                context.write(word, one);
            }
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
        // Configuration
        System.setProperty("hadoop.home.dir", "C:\\hadoop" );
        Configuration conf = new Configuration();
        output_config(args, conf);
        String[] parsedArgs = mapper_config(args, conf);

        // Job
        Job job = Job.getInstance(conf, "TrendingTopic.HashtagCount");

        // MAP0: Trending Topics
//        Configuration mapConf0 = new Configuration(false);
//        ChainMapper.addMapper(job, TrendingTopicsMapper.class, Object.class, Text.class,
//                Text.class, LongWritable.class,  mapConf0);

        // MAP1: Tweet to lowercase
//        Configuration mapConf1 = new Configuration(false);
//        ChainMapper.addMapper(job, LowerCaseMapper.class, Object.class, Text.class,
//                Text.class, LongWritable.class,  mapConf1);

        // MAP2: Clean missing data (text, hashtags)
//        Configuration mapConf2 = new Configuration(false);
//        ChainMapper.addMapper(job, MissingFieldsMapper.class, Object.class, Text.class,
//                Text.class, LongWritable.class,  mapConf2);

        // MAP3: Clean missing data (text, hashtags)
//        Configuration mapConf3 = new Configuration(false);
//        ChainMapper.addMapper(job, RedundantFieldsMapper.class, Object.class, Text.class,
//                Text.class, LongWritable.class,  mapConf3);

        // MAP4: Clean missing data (text, hashtags)
        Configuration mapConf4 = new Configuration(false);
        ChainMapper.addMapper(job, SpecificLangMapper.class, Object.class, Text.class,
                Text.class, LongWritable.class, mapConf4);

        // Reducer conf
        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, LongSumReducer.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, reduceConf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Treatment
        FileInputFormat.addInputPath(job, new Path(parsedArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(parsedArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}