import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondSort {
    public static class SecondSortMapper extends Mapper<Text, Text, SSPair, IntWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // Input as key: first number, value: second number
            // Output as key: SSPair, value: IntWritable
            int first = Integer.parseInt(key.toString());
            int second = Integer.parseInt(value.toString());
            context.write(new SSPair(first, second), new IntWritable(second));
        }
    }

    public static class SecondSortPartitioner extends HashPartitioner<SSPair, IntWritable> {
        @Override
        public int getPartition(SSPair key, IntWritable value, int numReduceTasks) {
            // Distribute keys according to the first number
            return key.getFirst() % numReduceTasks;
        }
    }

    public static class SSPair implements WritableComparable<SSPair> {
        private int first;
        private int second;
        public SSPair(){
            super();
        }
        public SSPair(int f, int s){
            super();
            first = f;
            second = s;
        }
        public int getFirst(){
            return first;
        }
        public int getSecond(){
            return second;
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
            first = arg0.readInt();
            second = arg0.readInt();
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            arg0.writeInt(first);
            arg0.writeInt(second);
        }

        @Override
        public int compareTo(SSPair pair) {
            int flag = first - pair.getFirst();// In asc order
            if (flag == 0) {
                return pair.getSecond() - second;// In desc order
            }
            return flag;
        }
    }

    public static class SecondSortGroupingComparator extends WritableComparator {
        public SecondSortGroupingComparator() {
            super(SSPair.class, true);
        }

        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            // Combining pairs into groups
            SSPair pair1 = (SSPair) wc1;
            SSPair pair2 = (SSPair) wc2;
            return Integer.compare(pair1.getFirst(), pair2.getFirst());
        }
    }

    public static class SecondSortReducer extends Reducer<SSPair, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(SSPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Output all sorted pairs in key: first number, value: second number
            for(IntWritable value : values){
                context.write(new IntWritable(key.getFirst()), value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "SecondSort");
        job.setJarByClass(SecondSort.class);
        job.setMapperClass(SecondSort.SecondSortMapper.class);
        job.setPartitionerClass(SecondSort.SecondSortPartitioner.class);
        job.setReducerClass(SecondSort.SecondSortReducer.class);
        job.setGroupingComparatorClass(SecondSortGroupingComparator.class);

        job.setMapOutputKeyClass(SSPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}