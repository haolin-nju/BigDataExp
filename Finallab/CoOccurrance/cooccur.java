import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class cooccur {
    public static class PairOfText implements WritableComparable<PairOfText> {
        private Text fname;
        private Text sname;
        PairOfText() {
            set(new Text(),new Text());//基本类型int，long等可以不用初始化，但是对象类型变量一定要new，因为反序列化是要读取数据到first和second，会出现空指针引用的问题。
        }
        void set(Text first, Text second) {
            this.fname = first;
            this.sname = second;
        }
        public Text getFirst() {
            return fname;
        }
        public Text getSecond() {
            return sname;
        }
        @Override
        public void write(DataOutput out)throws IOException {
            fname.write(out);
            sname.write(out);
        }
        @Override
        public void readFields(DataInput in)throws IOException {
            fname.readFields(in);
            sname.readFields(in);
        }
        public int compareTo(PairOfText o) {
            int cmp = fname.compareTo(o.fname);
            if(cmp !=0)
                return cmp;
            else
                return sname.compareTo(o.sname);
        }

    }
    public static class CooccurMapper extends Mapper<LongWritable, Text, PairOfText, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            //Set value
            String[] line = value.toString().split(" ");
            Set<String> set=new HashSet<String>(Arrays.asList(line));
            String[] newline = new String[set.size()];
            int i = 0;
            for(String str: set){
                newline[i] = str;
                i ++;
            }
            i = 0;
            for(; i < newline.length; i ++){
                for(int j = i + 1; j < newline.length; j ++){
                    PairOfText addpair = new PairOfText();
                    addpair.set(new Text(newline[i]), new Text(newline[j]));
                    context.write(addpair, new IntWritable(1));
                }
            }
        }
    }

    public static class CooccurReducer extends Reducer<PairOfText, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(PairOfText key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v : values) {
                sum += v.get();
            }
            context.write(new Text("<"+key.getFirst().toString()+","+key.getSecond().toString()+">"), new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "co_occurrance");
        job.setJarByClass(cooccur.class);

        job.setMapperClass(cooccur.CooccurMapper.class);
        job.setReducerClass(cooccur.CooccurReducer.class);

        job.setMapOutputKeyClass(PairOfText.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);// read by row so that output by row
        job.setOutputFormatClass(TextOutputFormat.class);// output by row to each file

        job.setNumReduceTasks(5);// because there are 5 novels for JinYong

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}