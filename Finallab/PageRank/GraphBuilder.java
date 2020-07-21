import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.Scanner;

public class GraphBuilder {
    public static class GraphBuilderMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // just set 1 to the cur_rank before link_list
            context.write(key, new Text("1" + value));
        }
    }

    public static long main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        //ref Book P88-89
        FileSystem hdfs = FileSystem.get(conf);
        long row_cnt = 0;
        String str;
        FSDataInputStream in = null;
        Scanner scan;
        try {
            FileStatus[] stats = hdfs.listStatus(new Path(args[0]));
            row_cnt = 0;
            for(int i=0;i<stats.length;i++){
                in = hdfs.open(stats[i].getPath());
                scan = new Scanner(in);
                while(scan.hasNext()){
                    str = scan.nextLine();
                    row_cnt++;
                }
                scan.close();
                in.close();
            }
        }
        catch(IOException e){
            e.printStackTrace();
        }
        System.out.println(row_cnt);

        Job job = new Job(conf, "Graph Builder");
        job.setJarByClass(GraphBuilder.class);

        job.setMapperClass(GraphBuilder.GraphBuilderMapper.class);
//        job.setReducerClass(GraphBuilder.GraphBuilderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);// read by row so that output by row
        job.setOutputFormatClass(TextOutputFormat.class);// output by row to each file

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return row_cnt;
    }
}
