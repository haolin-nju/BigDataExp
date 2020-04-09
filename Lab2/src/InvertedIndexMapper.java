import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    // 输入键：行号，输入值：一行空格分隔的词语，输出键：词语和文档名，输出值：词频
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> hashMap = new HashMap<>();
        // 解析小说名
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().split("\\.txt|\\.TXT")[0];
        StringTokenizer itr = new StringTokenizer(value.toString());
        // 使用HashMap统计词频
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
            context.write(new Text(entry.getKey() + "," + fileName), new IntWritable(entry.getValue()));
        }
    }
}