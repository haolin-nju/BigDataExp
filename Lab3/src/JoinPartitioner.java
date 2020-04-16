import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class JoinPartitioner extends HashPartitioner<Order, NullWritable> {
    @Override
    // 按照pid进行分配
    public int getPartition(Order key, NullWritable value, int numReduceTasks) {
        return key.getPid() % numReduceTasks;
    }
}