import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/* For constructing the solution */

public class AirQualityMonitor {

    public static final String LEVEL_0 = "Invalid data";
    public static final String LEVEL_1 = "Good";
    public static final String LEVEL_2 = "Moderate";
    public static final String LEVEL_3 = "Unhealthy for Sensitive Group";
    public static final String LEVEL_4 = "Unhealthy";
    public static final String LEVEL_5 = "Very Unhealthy";
    public static final String LEVEL_6 = "Hazardous";

    public static class AirQualityMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text output = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            /* get the file name */
//            FileSplit fileSplit = (FileSplit) context.getInputSplit();
//            String filename = fileSplit.getPath().getName();
//            context.write(new Text(filename), new Text(""));

            StringTokenizer itr = new StringTokenizer(value.toString()); // each time the value is a different line
            String ym = itr.nextToken(); // get the first column - year month indicator
            int yearMonth = Integer.parseInt(ym);

            if (yearMonth == 0) {
                return; // skip the first row 0
            } else {
                while (itr.hasMoreTokens()) {
                    String sVal = itr.nextToken(); // get the actual value
                    int val = Integer.parseInt(sVal);
                    if (val >= 0 && val <= 49) {
                        output.set("Good");
                    } else if (val >= 50 && val <= 99) {
                        output.set("Moderate");
                    } else if (val >= 100 && val <= 149) {
                        output.set("Unhealthy for Sensitive Group");
                    } else if (val >= 150 && val <= 199) {
                        output.set("Unhealthy");
                    } else if (val >= 200 && val <= 299) {
                        output.set("Very Unhealthy");
                    } else if (val >= 300) {
                        output.set("Hazardous");
                    } else if (val == -1) {
                        output.set("invalid data");
                    }
                    context.write(new Text(ym), output);
                }

            }


        }
    }

    public static class AirQualityReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, new Text(val));
            }
            // for each val in values
            // context.write(key, val)
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: AirQuality <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Air Quality Monitor by 16251164");

        //To force machines in Hadoop to use the jar on the HDFS
        //Assume your jar name is AirQuality.jar
        //Please change "bchoi" in "/home/comp/bchoi/AirQuality.jar" to your a/c name
        // also ensure that you have uploaded your jar to HDFS:
        // hadoop fs -put AirQuality.jar
        job.addFileToClassPath(new Path("/home/comp/e6251164/AirQualityMonitor.jar"));
        job.setJarByClass(AirQualityMonitor.class);
        job.setMapperClass(AirQualityMapper.class);
        job.setReducerClass(AirQualityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputDirRecursive(job, true);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
