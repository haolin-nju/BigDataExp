import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        // 按照词语Partition
        return super.getPartition(new Text(key.toString().split(",")[0]), value, numReduceTasks);
    }
}