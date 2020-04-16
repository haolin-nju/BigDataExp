import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "join");
        job.setJarByClass(Join.class);
        job.setMapperClass(JoinMapper.class);
        job.setPartitionerClass(JoinPartitioner.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Order.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(5);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}