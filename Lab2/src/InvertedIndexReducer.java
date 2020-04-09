import java.io.IOException;
import java.util.LinkedList;
import java.util.StringJoiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    // 元素格式"文档名:词频"
    private LinkedList<String> list;
    // 上一个词语
    private String prev;
    // 上一个词语的词频
    private int freq;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        list = new LinkedList<>();
        prev = "";
        freq = 0;
    }

    @Override
    // 输入键：词语和文档名，输入值：每行的词频，输出键：词语，输出值：平均出现次数和带词频索引
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String current = key.toString().split(",")[0];
        String fileName = key.toString().split(",")[1];
        // 遇到新词，输出上一个词语相关信息
        if (!current.equals(prev) && !prev.isEmpty()) {
            StringJoiner joiner = new StringJoiner("; ");
            for (String s : list) {
                joiner.add(s);
            }
            // list的大小即为文档数，freq除以它得到平均出现次数
            context.write(new Text(prev),
                    new Text(String.format("%.2f, ", (float) freq / list.size()) + joiner.toString()));
            list.clear();
            freq = 0;
        }
        // 当前词语在当前文档中的词频
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        list.add(fileName + ":" + String.valueOf(sum));
        // 更新上一个词语和上一个词语的词频
        prev = current;
        freq += sum;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!prev.isEmpty()) {
            StringJoiner joiner = new StringJoiner("; ");
            for (String s : list) {
                joiner.add(s);
            }
            context.write(new Text(prev),
                    new Text(String.format("%.2f, ", (float) freq / list.size()) + joiner.toString()));
        }
    }
}