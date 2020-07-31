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

public class Preprocess {
    public static class PreprocessMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        Set<String> nameSet = new HashSet<String>();//人名集合

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            // Get all names in name list
            Configuration conf = context.getConfiguration();
            String nameFile = conf.get("nameFile");
            FileSystem fs = FileSystem.get(conf);
            Path remotePath = new Path(nameFile);
            FSDataInputStream in = fs.open(remotePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = br.readLine()) != null) {
//                if(line.equals("汉子") || line.equals("大汉") || line.equals("胖子") || line.equals("渔人")
//                        || line.equals("农夫") || line.equals("瘦子") || line.equals("铁匠") || line.equals("童子") || line.equals("农妇")){
//                    continue;
//                }
                nameSet.add(line);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Key: Nullwritable, Value: name list for cur line
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            String authorName = fileName.split("\\d+")[0];

            // must exclude authors other than JinYong
            if (authorName.equals("金庸")) {
                // Set value
                String[] line = value.toString().split(" ");
                String lineNames = "";
                int cnt = 0;
                for (String str : line) {
                    if (nameSet.contains(str)) {
                        lineNames += (str + " ");
                        cnt += 1;
                    }
                }

                //If this line has >=2 names, then emit key-val
                //If this line has <2 names, ignore it
                if (cnt >= 2) {
                    context.write(NullWritable.get(), new Text(lineNames.trim()));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("nameFile", args[0]);//人名列表文件路径
        Job job = new Job(conf, "Preprocess");
        job.setJarByClass(Preprocess.class);

        job.setMapperClass(Preprocess.PreprocessMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

        // Three path args, first: nameList, second: novels, third: outputDir
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
