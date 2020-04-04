import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexPartitioner<K,V> extends HashPartitioner<K,V>
{
    @Override
    public int getPartition(K key, V value, int numReduceTasks)
    {
        //K term = key.toString().split(",")[0]; //<term, docid>=>term
        //super.getPartition(term, value, numReduceTasks);
    }
}
