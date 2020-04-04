import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.FileSplit;

public class InvertedIndexMapper extends Mapper<Text, Text, Text, Text>{
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
        FileSplit fileSplit= (FileSplit)context.getInputSplit();
        String fileName= fileSplit.getPath().getName();
        Text word = new Text();
        Text fileName_lineOffset= new Text(fileName+"#"+key.toString());
        StringTokenizer itr= new StringTokenizer(value.toString());
        for(; itr.hasMoreTokens(); ) {
            word.set(itr.nextToken());
            context.write(word, fileName_lineOffset);
        }
    }
}
