import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.Scanner;

public class DegreeCentrality {
    public static class DegreeCentralityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tmp = value.toString().replace(" ", "").replace("\t", "");
            String[] line = tmp.split("<|>|,");
            context.write(new Text(line[1]), new IntWritable(Integer.valueOf(line[3])));
        }
    }

    public static class DegreeCentralityReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private static int sum_degree;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sum_degree = Integer.valueOf(context.getConfiguration().get("Line Count"));
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double degree_cnt = 0;
            for (IntWritable value : values) {
                degree_cnt += value.get();
            }
            double dc = degree_cnt / (sum_degree - 1);
            context.write(key, new DoubleWritable(dc));
        }
    }

    public static void main(String args[], int row_cnt) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("Line Count", String.valueOf(row_cnt));

        Job job = new Job(conf, "Degree Centrality");
        job.setJarByClass(DegreeCentrality.class);

        job.setMapperClass(DegreeCentrality.DegreeCentralityMapper.class);
        job.setReducerClass(DegreeCentrality.DegreeCentralityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);// read by row so that output by row
        job.setOutputFormatClass(TextOutputFormat.class);// output by row to each file

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
