import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> hashMap = new HashMap<>();
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().split("\\.txt")[0];
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
            context.write(new Text(entry.getKey() + "," + fileName), new IntWritable(entry.getValue()));
        }
    }
}