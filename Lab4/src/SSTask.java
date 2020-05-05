package src;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SSTask {
    public static class SSKey implements WritableComparable<SSKey> {
        private int first;
        private int second;
        void set(int f, int s){
            first = f;
            second = s;
        }
        int getFirst(){
            return first;
        }
        int getSecond(){
            return second;
        }
        @Override
        public int compareTo(SSKey o) {
            if ( first != o.first )
            {
                return first < o.first ? -1 : 1;
            }
            else if ( second != o.second )
                return second < o.second ? 1 : -1;
            else
                return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(first);
            dataOutput.writeInt(second);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            first = dataInput.readInt();
            second = dataInput.readInt();
        }

    }

    public static class SSMapper extends Mapper<Text, Text, SSKey, IntWritable> {
        // use KeyValueTextInputFormat
        private SSKey tmp = new SSKey();
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString()!=null && !key.toString().isEmpty()) {
                int f = Integer.parseInt(key.toString());
                int s = Integer.parseInt(value.toString());
                tmp.set(f, s);
                context.write(tmp, new IntWritable(s));
            }
        }
    }
    public static class SSPartitioner extends Partitioner<SSKey, IntWritable> {
        public int getPartition(SSKey key, IntWritable value, int numReduceTasks) {
            return (new Text(String.valueOf(key.getFirst())).hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
    public static class SSGroupingComparator extends WritableComparator {
        protected SSGroupingComparator()
        {
            super(SSKey.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            SSKey a = (SSKey) w1;
            SSKey b = (SSKey) w2;
            return Integer.compare(a.getFirst(), b.getFirst());
        }
    }

    public static class SSReducer extends Reducer<SSKey, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(SSKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(new IntWritable(key.getFirst()), value);
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "SecondSort");
        job.setJarByClass(SSTask.class);

        job.setMapperClass(SSMapper.class);
        job.setReducerClass(SSReducer.class);
        job.setPartitionerClass(SSPartitioner.class);
        job.setGroupingComparatorClass(SSGroupingComparator.class);

        job.setMapOutputKeyClass(SSKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
