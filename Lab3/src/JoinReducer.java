import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Order, NullWritable, Order, NullWritable> {
    int pid;
    String pname;
    int price;

    @Override
    protected void reduce(Order key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        // 相同pid的记录中，来自product.txt的最先出现
        if (key.getPid() != pid) {
            pid = key.getPid();
            pname = key.getPname();
            price = key.getPrice();
        } else {
            key.setPname(pname);
            key.setPrice(price);
            context.write(key, NullWritable.get());
        }
    }
}