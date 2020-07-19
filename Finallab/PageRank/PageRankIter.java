import org.apache.hadoop.conf.Configuration;
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

public class PageRankIter {
    private static int row_cnt = 0;
    public static class PageRankIterMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            //Input: key: name, value: oldRank + linklist
            //Output: key: name, value: newRank(doesn't have '[') or linklist(has '[')
            String[] rank_and_list = value.toString().split("\\s+");
            double cur_rank = new Double(rank_and_list[0]);
            String link_list = rank_and_list[1];
            String[] arr = link_list.substring(1, link_list.length() - 1).split("\\|");
            int list_len = arr.length;
            for (String ss : arr) {
                context.write(new Text(ss.split(",")[0]), new Text(Double.toString(cur_rank / list_len)));
            }
            context.write(key, new Text(link_list));
            row_cnt += 1;
        }
    }
    public static class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {
        private static double d = 0.85;// ref: PPT Ch8 Page15
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Input: key: name, value: newRank(doesn't have '[') or linklist(has '[')
            //Output: key: name, value: newRank + linklist
            String value = "";
            double val = 0;
            for(Text t : values){
                String cur_text = t.toString();
                if(cur_text.substring(0,1).equals("[")){// means link_list
                    value = cur_text;
                }
                else{
                    val += new Double(cur_text);
                }
            }
            double new_rank = (1 - d) / row_cnt + val * d;
            context.write(key, new Text(new_rank + " " + value));
        }
    }
    public static void iter(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "PageRank Iter");
        job.setJarByClass(PageRankIter.class);

        job.setMapperClass(PageRankIter.PageRankIterMapper.class);
        job.setReducerClass(PageRankIter.PageRankIterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);// read by row so that output by row
        job.setOutputFormatClass(TextOutputFormat.class);// output by row to each file

        job.setNumReduceTasks(5);// because there are 5 novels for JinYong

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
