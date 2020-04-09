import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
    @Override
    // 输入键：词频，输入值：相同词频的所有词语，输出键：词语，输出值：词频
    protected void reduce(FloatWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}