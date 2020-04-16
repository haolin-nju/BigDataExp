import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<LongWritable, Text, Order, NullWritable> {
    @Override
    // 根据文本名确定flag，调用Order构造函数
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String[] buffer = value.toString().split(" ");
        Order order = new Order(buffer, fileName.equals("order.txt"));
        context.write(order, NullWritable.get());
    }
}