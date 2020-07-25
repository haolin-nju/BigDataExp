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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

public class EigenVecCentrality {
    public static class EigenVecCentralityMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static int row_cnt;
        private static int sum;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            row_cnt = Integer.valueOf(context.getConfiguration().get("Line Count"));
//            sum = row_cnt;
            sum = Integer.valueOf(context.getConfiguration().get("Sum"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tmp = value.toString().replace(" ", "").replace("\t", "");
            String[] line = tmp.split("<|>|,");
            context.write(new Text(line[1]), new Text(line[2] + "," + 1.0));
//            context.write(new Text(line[1]), new Text(line[2] + "," + (Double.valueOf(line[3]) / sum)));
        }
    }

    public static class EigenVecCentralityReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private static int row_cnt;
        private static HashMap<String, Double> vector = new HashMap<String, Double>();
        private static HashMap<String, String> link_list = new HashMap<String, String>();
        private static HashMap<String, Double> weight_list = new HashMap<String, Double>();

        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            row_cnt = Integer.valueOf(context.getConfiguration().get("Line Count"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String key_str = String.valueOf(key);
            String neighbors = "";
            double weight = 0;
            int nei_cnt = 0;
            for (Text value : values) {
                String[] str_arr = String.valueOf(value).split(",");
                neighbors += str_arr[0] + ",";
                weight += Double.valueOf(str_arr[1]);
//                nei_cnt++;
            }
            vector.put(key_str, weight);
            link_list.put(key_str, neighbors.substring(0, neighbors.length() - 1));//ignore last ","
        }

        @Override
        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            double sum = 0;
            double delta_sum = 0;
            for (int iter = 0; iter < 100; ++iter) {
                sum = 0;
                delta_sum = 0;
                for (HashMap.Entry<String, String> m1 : link_list.entrySet()) {
                    String[] link_arr = m1.getValue().split(",");
                    double cur_weight = vector.get(m1.getKey());
                    for (int i = 0; i < link_arr.length; ++i) {
                        if (weight_list.containsKey(link_arr[i]) == false) {
                            weight_list.put(link_arr[i], 0.0);
                        }
                        weight_list.put(link_arr[i], weight_list.get(link_arr[i]) + cur_weight);
                        sum += cur_weight;
                    }
                }
                for (HashMap.Entry<String, Double> m2 : weight_list.entrySet()) {
//                    double m2_val = (m2.getValue() / sum) / link_list.get(m2.getKey()).split(",").length;
                    double m2_val = m2.getValue() / sum;
                    delta_sum += Math.abs(vector.get(m2.getKey()) - m2_val);
                    weight_list.put(m2.getKey(), m2_val);
                }
                vector = weight_list;
                weight_list = new HashMap<String, Double>();
                if (delta_sum < 1e-9) {
                    System.out.println(iter);
                    break;
                }
            }
            for (HashMap.Entry<String, Double> m2 : vector.entrySet()) {
                context.write(new Text(m2.getKey()), new DoubleWritable(m2.getValue()));
            }
        }
    }

    public static void main(String args[], Integer[] int_arr) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("Line Count", String.valueOf(int_arr[0]));
        conf.set("Sum", String.valueOf(int_arr[1]));

        Job job = new Job(conf, "EigenVector Centrality");
        job.setJarByClass(EigenVecCentrality.class);

        job.setMapperClass(EigenVecCentrality.EigenVecCentralityMapper.class);
        job.setReducerClass(EigenVecCentrality.EigenVecCentralityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
