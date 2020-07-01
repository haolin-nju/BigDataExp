import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.*;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class preprocess {
    public static class PreprocessMapper extends Mapper<LongWritable, Text, Text, Text>{
        Set<String> nameSet = new HashSet<String>();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
//            String inputDir = ((FileSplit)context.getInputSplit()).getPath().getParent().getParent().toUri().getPath();
            String nameFile = conf.get("nameFile");
            FileSystem fs = FileSystem.get(conf);
            Path remotePath = new Path(nameFile);
            FSDataInputStream in = fs.open(remotePath);
//            FileReader fr = new FileReader(nameFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while((line = br.readLine()) != null){
//                System.out.println(line);
                nameSet.add(line);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            String authorName = fileName.split("\\d+")[0];

            if (authorName.equals("金庸")) {// 必须排除不是金庸的小说
                //Set key
                String novelName = fileName.replaceAll("\\D+", "");

                //Set value
                String[] line = value.toString().split(" ");
                String lineNames = "";
                int cnt = 0;
                for (String str : line) {
                    if (nameSet.contains(str)) {
                        lineNames += (str + " ");
                        cnt += 1;
                    }
                }

                //If this line has names, then emit key-val
                if (cnt >= 2) {
//                System.out.println(novelName + ": " + lineNames);
                    context.write(new Text(novelName), new Text(lineNames.trim()));
                }
            }
        }
    }

    public static class PreprocessReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t : values) {
                context.write(t, NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("nameFile", args[0]);
        Job job = new Job(conf, "data_preprocess");
        job.setJarByClass(preprocess.class);

        job.setMapperClass(preprocess.PreprocessMapper.class);
        job.setReducerClass(preprocess.PreprocessReducer.class);

        job.setMapOutputKeyClass(Text.class);// file name
        job.setMapOutputValueClass(Text.class);// paragraph texts
        job.setOutputKeyClass(Text.class);// paragraph texts
        job.setOutputValueClass(NullWritable.class);// null

        job.setInputFormatClass(TextInputFormat.class);// read by row so that output by row
        job.setOutputFormatClass(TextOutputFormat.class);// output by row to each file

        job.setNumReduceTasks(15);// because there are 15 novels for JinYong

        // Three path args, first: nameList, second: novels, third: outputDir
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
