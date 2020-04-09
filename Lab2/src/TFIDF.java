import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.LinkedList;

public class TFIDF {
    public static class TFIDFMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        // 输入键：行号，输入值：一行空格分隔的词语，输出键：词语和作者，输出值：词频
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            HashMap<String, Integer> hashMap = new HashMap<>();
            // 章节序号前面是作者
            String author = fileSplit.getPath().getName().split("\\d+")[0];
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String k = itr.nextToken();
                if (hashMap.containsKey(k)) {
                    Integer v = hashMap.get(k);
                    hashMap.put(k, ++v);
                } else {
                    hashMap.put(k, 1);
                }
            }
            for (HashMap.Entry<String, Integer> entry : hashMap.entrySet()) {
                context.write(new Text(entry.getKey() + "," + author), new IntWritable(entry.getValue()));
            }
        }
    }

    public static class TFIDFPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            // 按照词语Partition
            return super.getPartition(new Text(key.toString().split(",")[0]), value, numReduceTasks);
        }
    }

    public static class TFIDFReducer extends Reducer<Text, IntWritable, Text, Text> {
        // 元素格式"词语, 作者, TF"，保存某个词语所有相关信息
        private LinkedList<String> list;
        // 上一个词语
        private String Word;
        // 上一个作者
        private String Author;
        // 包含词语的文档数
        private int cnt;
        // TF
        private int tf;
        // 语料库文档总数
        private int total;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            list = new LinkedList<>();
            Word = "";
            Author = "";
            cnt = 0;
            tf = 0;
            Configuration conf = context.getConfiguration();
            total = Integer.parseInt(conf.get("total"));
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String word = key.toString().split(",")[0];
            String author = key.toString().split(",")[1];

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            boolean changed = false;
            // 作者不同，存进列表
            if (!Author.equals(author) && !Author.isEmpty()) {
                list.add(Author + ", " + Word + ", " + tf + "-");
                tf = 0;
                changed = true;
            }
            // 词语不同，需要输出列表
            if (!Word.equals(word) && !Word.isEmpty()) {
                // 作者相同才要存进列表，作者不同上个if已经存了
                if (!changed) {
                    list.add(Author + ", " + Word + ", " + tf + "-");
                    tf = 0;
                }
                // 计算IDF，输出列表
                double idf = Math.log((double) total / (cnt + 1));
                for (String str : list) {
                    context.write(new Text(str + String.valueOf(idf)), new Text());
                }
                list.clear();
                cnt = 0;
            }
            // 更新缓存数据
            Word = word;
            Author = author;
            cnt++;
            tf += sum;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 处理最后一个词语
            list.add(Author + ", " + Word + ", " + tf + "-");
            tf = 0;
            double idf = Math.log((double) total / (cnt + 1));
            for (String str : list) {
                context.write(new Text(str + String.valueOf(idf)), new Text());
            }
            list.clear();
            cnt = 0;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] stats = hdfs.listStatus(new Path(args[0]));
        int DocSum = stats.length;
        hdfs.close();
        // 全局变量传递
        conf.set("total", String.valueOf(DocSum));

        Job job = Job.getInstance(conf, "TFIDF");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDF.TFIDFMapper.class);
        job.setPartitionerClass(TFIDF.TFIDFPartitioner.class);
        job.setReducerClass(TFIDF.TFIDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
