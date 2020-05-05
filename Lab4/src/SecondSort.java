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
    public static class SecondSortMapper extends Mapper<Text, Text, SSPair, NullWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // Input as key: first number, value: second number
            // Output as key: SSPair, value: NullWritable
            String buffer[] = new String[2];
            buffer[0] = key.toString();
            buffer[1] = value.toString();
            SSPair pair = new SSPair(buffer);
            context.write(pair, NullWritable.get());
        }
    }

    public static class SecondSortPartitioner extends HashPartitioner<SSPair, NullWritable> {
        @Override
        public int getPartition(SSPair key, NullWritable value, int numReduceTasks) {
            // Distribute keys according to first number
            return key.getFirst() % numReduceTasks;
        }
    }

    public static class SSPair implements WritableComparable<SSPair> {
        private int first;
        private int second;
        public SSPair(){
            super();
        }
        public SSPair(String[] buffer){
            super();
            first = Integer.parseInt(buffer[0]);
            second = Integer.parseInt(buffer[1]);
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

    public static class SecondSortReducer extends Reducer<SSPair, NullWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(SSPair key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            // Output all sorted pairs in key: first number, value: second number
            for(NullWritable value : values){
                context.write(new IntWritable(key.getFirst()), new IntWritable(key.getSecond()));
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
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
