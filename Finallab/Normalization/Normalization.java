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

import java.io.IOException;
import java.io.*;
import java.util.ArrayList;

public class Normalization {
    public static class PairOfValue implements WritableComparable<PairOfValue> {
        private Text name;
        private Integer value;
        PairOfValue() {
            set(new Text(),0);//基本类型int，long等可以不用初始化，但是对象类型变量一定要new，因为反序列化是要读取数据到first和second，会出现空指针引用的问题。
        }
        PairOfValue(PairOfValue o)
        {
            set(new Text(o.getFirst()),o.value);
        }
        void set(Text first, int second) {
            this.name = first;
            this.value = second;
        }
        public Text getFirst() {
            return new Text(name);
        }
        public Integer getSecond() {
            return value;
        }
        @Override
        public void write(DataOutput out)throws IOException {
            name.write(out);
            out.writeInt(value);
        }
        @Override
        public void readFields(DataInput in)throws IOException {
            name.readFields(in);
            value=in.readInt();
        }
        public int compareTo(PairOfValue o) {
            return name.compareTo(o.name);
        }
    }
    public static class NormalizationMapper extends Mapper<LongWritable, Text, Text, PairOfValue>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit)context.getInputSplit();
            //Set value
            String tmp=value.toString().replace(" ","");
            tmp=tmp.replace("\t","");
            String[] line = tmp.split("<|>|,");
            PairOfValue OnePair=new PairOfValue();
            OnePair.set(new Text(line[2]),Integer.parseInt(line[3]));
            context.write(new Text(line[1]),OnePair);
        }
    }
    public static class NormalizationReducer extends Reducer<Text, PairOfValue, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<PairOfValue> values, Context context) throws IOException, InterruptedException {
            int sum = 0;//sum of connections
            Text OutPutkey=new Text(" [");
            ArrayList<PairOfValue> saveValues=new ArrayList<PairOfValue>();
            int num=0;
            for(PairOfValue v : values) {
                sum += v.getSecond();
                saveValues.add(new PairOfValue(v));
                num++;
            }
            int i=0;
            for(PairOfValue v : saveValues){
                ++i;
                if(i!=num)
                    OutPutkey.set(OutPutkey.toString()+v.getFirst().toString()+","+Float.toString((float)v.getSecond()/sum)+'|');
                else
                    OutPutkey.set(OutPutkey.toString()+v.getFirst().toString()+","+Float.toString((float)v.getSecond()/sum)+']');
            }
            context.write(new Text(key),OutPutkey);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Normalization");
        job.setJarByClass(Normalization.class);
        job.setMapperClass(Normalization.NormalizationMapper.class);
        job.setReducerClass(Normalization.NormalizationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);// read by row so that output by row
        job.setOutputFormatClass(TextOutputFormat.class);// output by row to each file
        job.setNumReduceTasks(5);// because there are 5 novels for JinYong
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
