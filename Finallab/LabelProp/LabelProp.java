import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LabelProp {
    private static int iters;
    private static int classes;
    private static HashMap<String, Integer> dict;
    private static String[] books;
    static {
        iters = 25;
        classes = 15;
        books = new String[] { "", "飞狐外传", "雪山飞狐", "连城诀", "天龙八部", "射雕英雄传", "白马啸西风", "鹿鼎记", "笑傲江湖", "书剑恩仇录", "神雕侠侣",
                "侠客行", "倚天屠龙记", "碧血剑", "鸳鸯刀", "越女剑" };
        dict = new HashMap<String, Integer>();
        dict.put("胡斐", 1);
        dict.put("程灵素", 1);
        dict.put("袁紫衣", 1);
        dict.put("苗若兰", 2);
        dict.put("胡一刀", 2);
        dict.put("狄云", 3);
        dict.put("戚芳", 3);
        dict.put("水笙", 3);
        dict.put("乔峰", 4);
        dict.put("段誉", 4);
        dict.put("虚竹", 4);
        dict.put("郭靖", 5);
        dict.put("黄蓉", 5);
        dict.put("杨康", 5);
        dict.put("李文秀", 6);
        dict.put("苏普", 6);
        dict.put("阿曼", 6);
        dict.put("韦小宝", 7);
        dict.put("双儿", 7);
        dict.put("风际中", 7);
        dict.put("令狐冲", 8);
        dict.put("任盈盈", 8);
        dict.put("岳灵珊", 8);
        dict.put("陈家洛", 9);
        dict.put("霍青桐", 9);
        dict.put("喀丝丽", 9);
        dict.put("杨过", 10);
        dict.put("小龙女", 10);
        dict.put("金轮法王", 10);
        dict.put("石破天", 11);
        dict.put("白万剑", 11);
        dict.put("丁珰", 11);
        dict.put("张无忌", 12);
        dict.put("赵敏", 12);
        dict.put("周芷若", 12);
        dict.put("袁承志", 13);
        dict.put("夏雪宜", 13);
        dict.put("何红药", 13);
        dict.put("林玉龙", 14);
        dict.put("任飞燕", 14);
        dict.put("常长风", 14);
        dict.put("阿青", 15);
        dict.put("范蠡", 15);
        dict.put("西施", 15);
    }

    public static class LabelPropPreMapper extends Mapper<LongWritable, Text, LabelPropObject, NullWritable> {
        private LabelPropObject lpobj = new LabelPropObject();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            lpobj.fromString1(value.toString());
            context.write(lpobj, NullWritable.get());
            lpobj.link = "null";
            int label = dict.get(lpobj.name) == null ? 0 : dict.get(lpobj.name);
            for (int i = 1; i <= classes; ++i) {
                lpobj.label = i;
                lpobj.prob = i == label ? 1.0 : 0.0;
                context.write(lpobj, NullWritable.get());
            }
        }
    }

    public static class LabelPropIterMapper extends Mapper<LongWritable, Text, LabelPropObject, DoubleWritable> {
        private LabelPropObject lpobj = new LabelPropObject();
        private ArrayList<String> neigh = new ArrayList<>();
        private ArrayList<Double> weight = new ArrayList<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            lpobj.fromString2(value.toString());
            if (lpobj.label == 0) {
                neigh.clear();
                weight.clear();
                String[] buffer = lpobj.link.substring(1, lpobj.link.length() - 1).split("\\|");
                for (String edge : buffer) {
                    int idx = edge.indexOf(',');
                    neigh.add(edge.substring(0, idx));
                    weight.add(Double.parseDouble(edge.substring(idx + 1)));
                }
                context.write(lpobj, new DoubleWritable(0.0));
                lpobj.link = "null";
            } else {
                for (int i = 0; i < neigh.size(); ++i) {
                    lpobj.name = neigh.get(i);
                    context.write(lpobj, new DoubleWritable(lpobj.prob * weight.get(i)));
                }
            }
        }
    }

    public static class LabelPropIterCombiner
            extends Reducer<LabelPropObject, DoubleWritable, LabelPropObject, DoubleWritable> {
        @Override
        public void reduce(LabelPropObject key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double prob = 0.0;
            for (DoubleWritable value : values)
                prob += value.get();
            context.write(key, new DoubleWritable(prob));
        }
    }

    public static class LabelPropIterReducer
            extends Reducer<LabelPropObject, DoubleWritable, LabelPropObject, NullWritable> {
        @Override
        public void reduce(LabelPropObject key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            if (dict.get(key.name) != null)
                key.prob = dict.get(key.name) == key.label ? 1.0 : 0.0;
            else if (key.label != 0) {
                double prob = 0.0;
                for (DoubleWritable value : values)
                    prob += value.get();
                key.prob = prob;
            }
            context.write(key, NullWritable.get());
        }
    }

    public static class LabelPropPostMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private LabelPropObject lpobj = new LabelPropObject();
        private String name;
        private int label;
        private double prob;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            lpobj.fromString2(value.toString());
            if (!lpobj.name.equals(name)) {
                if (name != null)
                    context.write(new IntWritable(label), new Text(name));
                name = lpobj.name;
                label = 0;
                prob = 0.0;
            } else if (lpobj.prob > prob) {
                label = lpobj.label;
                prob = lpobj.prob;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (name != null)
                context.write(new IntWritable(label), new Text(name));
        }
    }

    public static class LabelPropPostReducer extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Text book = new Text(books[key.get()]);
            for (Text value : values)
                context.write(value, book);
        }
    }

    public static void main(String[] args) throws Exception {
        long begin = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(LabelProp.class);
        job.setMapperClass(LabelPropPreMapper.class);
        job.setOutputKeyClass(LabelPropObject.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/data0"));
        job.waitForCompletion(true);

        for (int i = 0; i < iters; ++i) {
            job = new Job(conf);
            job.setJarByClass(LabelProp.class);
            job.setMapperClass(LabelPropIterMapper.class);
            job.setCombinerClass(LabelPropIterCombiner.class);
            job.setReducerClass(LabelPropIterReducer.class);
            job.setMapOutputKeyClass(LabelPropObject.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(LabelPropObject.class);
            job.setOutputValueClass(NullWritable.class);
            job.setNumReduceTasks(5);
            FileInputFormat.addInputPath(job, new Path(args[1] + "/data" + i));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/data" + (i + 1)));
            job.waitForCompletion(true);
        }

        job = new Job(conf);
        job.setJarByClass(LabelProp.class);
        job.setMapperClass(LabelPropPostMapper.class);
        job.setReducerClass(LabelPropPostReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1] + "/data" + iters));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/res"));
        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        System.out.println((end - begin) / 1000.0);
    }
}