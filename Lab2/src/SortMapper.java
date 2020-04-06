import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Object, Text, FloatWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] buffer = value.toString().split("\\s|,");
        context.write(new FloatWritable(Float.parseFloat(buffer[1])), new Text(buffer[0]));
    }
}