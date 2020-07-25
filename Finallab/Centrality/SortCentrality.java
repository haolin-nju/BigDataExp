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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortCentrality {
    public static class SortCentralityDoubleWritable implements WritableComparable<SortCentralityDoubleWritable> {
        private String str;
        private double pr;

        public SortCentralityDoubleWritable() {
            super();
            str = "";
            pr = 0;
        }

        public SortCentralityDoubleWritable(String s, double f) {
            super();
            str = s;
            pr = f;
        }

        public String getName() {
            return str;
        }

        public double getPr() {
            return pr;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(str);
            out.writeDouble(pr);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            str = in.readUTF();
            pr = in.readDouble();
        }

        @Override
        public int compareTo(SortCentralityDoubleWritable prdw) {
            double prdw_pr = prdw.getPr();
            String prdw_name = prdw.getName();
            return prdw_pr == pr ? prdw_name.compareTo(str) : (prdw_pr < pr ? -1 : 1);
        }
    }

    public static class SortCentralityMapper extends Mapper<Text, Text, SortCentralityDoubleWritable, NullWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            double cur_rank = new Double(value.toString());
            SortCentralityDoubleWritable prdw = new SortCentralityDoubleWritable(key.toString(), cur_rank);
            context.write(prdw, NullWritable.get());
        }
    }

    public static class SortCentralityReducer extends Reducer<SortCentralityDoubleWritable, NullWritable, Text, Text> {
        @Override
        protected void reduce(SortCentralityDoubleWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.getName()), new Text(Double.toString(key.getPr())));
        }
    }

    public static void main(String args[], String subjob) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Sort " + subjob + " Centrality");
        job.setJarByClass(SortCentrality.class);

        job.setMapperClass(SortCentrality.SortCentralityMapper.class);
        job.setReducerClass(SortCentrality.SortCentralityReducer.class);

        job.setMapOutputKeyClass(SortCentralityDoubleWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);// read by row so that output by row
        job.setOutputFormatClass(TextOutputFormat.class);// output by row to each file

        job.setNumReduceTasks(1);// because there are 1 novels for JinYong

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (subjob == "Degree") {
            job.waitForCompletion(true);
        }
        else {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
