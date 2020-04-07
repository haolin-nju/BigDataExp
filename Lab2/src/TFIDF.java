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
    public static class TFIDFMapper extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // function output: key<Author, filename, Term>, value<count>
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            HashMap<String, Integer> hashMap = new HashMap<>();
            String author = fileSplit.getPath().getName().split("[0-9]+")[0];
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
                context.write(new Text( entry.getKey() + "," + author ), new IntWritable(entry.getValue()));
            }
        }
    }

    public static class TFIDFPartitioner extends HashPartitioner<Text, IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            //对单词做partition
            return super.getPartition(new Text(key.toString().split(",")[0]), value, numReduceTasks);
        }
    }

    public static class TFIDFReducer extends Reducer<Text, IntWritable, Text, Text>{
        private String Word ;
        private String Author ;
        //how many docs
        private int cnt;
        //how many times
        private int tf;
        private int DocSum;
        private LinkedList<String> list;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            list = new LinkedList<>();
            Word = "";
            Author = "";
            cnt = 0;
            tf = 0;
            Configuration conf = context.getConfiguration();
            DocSum = Integer.parseInt(conf.get("DocSum"));

        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //在mapper或reducer中，

            String word = key.toString().split(",")[0];
            String author = key.toString().split(",")[1];

            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }

            int changed = 0;
            if (!Author.isEmpty() && !Author.equals(author)){
                // change an author
                list.add(Author + ", " + Word + ", " + tf + "-");
                tf = 0;
                changed = 1;
            }
            if (!Word.equals(word) && !Word.isEmpty()){
                // get a word's IDF then output all pairs of the word
                if ( changed == 0 ){
                    list.add(Author + ", " + Word + ", " + tf + "-");
                    tf = 0;
                }
                double idf = Math.log((double)DocSum/ (cnt + 1));
                for(String str: list){
                    context.write(new Text(str + String.valueOf(idf)), new Text());
                }
                list.clear();
                cnt = 0;
            }

            Word = word;
            Author = author;
            cnt ++;
            tf += sum;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // the last word
            list.add(Author + ", " + Word + ", " + tf + "-");
            tf = 0;
            double idf = Math.log((double)DocSum/ (cnt + 1));
            for(String str: list){
                context.write(new Text(str + String.valueOf(idf)), new Text());
            }
            list.clear();
            cnt = 0;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem hdfs=FileSystem.get(conf);
        FileStatus[] stats =hdfs.listStatus(new Path(args[0]));
        int DocSum = stats.length;
        hdfs.close();
        //全局变量传递
        conf.set("DocSum", String.valueOf(DocSum));

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
