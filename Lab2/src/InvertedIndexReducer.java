import java.io.IOException;
import java.util.LinkedList;
import java.util.StringJoiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    private LinkedList<String> list;
    private String prev;
    private int freq;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        list = new LinkedList<>();
        prev = null;
        freq = 0;
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        String current = key.toString().split(",")[0];
        String fileName = key.toString().split(",")[1];
        if (!current.equals(prev) && prev != null) {
            StringJoiner joiner = new StringJoiner("; ");
            for (String s : list) {
                joiner.add(s);
            }
            context.write(new Text(prev), new Text(String.format("%.2f, ", (float) freq / list.size()) + joiner.toString()));
            list.clear();
            freq = 0;
        }
        prev = current;
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        list.add(fileName + ":" + String.valueOf(sum));
        freq += sum;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (prev != null) {
            StringJoiner joiner = new StringJoiner("; ");
            for (String s : list) {
                joiner.add(s);
            }
            context.write(new Text(prev), new Text(String.format("%.2f, ", (float) freq / list.size()) + joiner.toString()));
        }
    }
}