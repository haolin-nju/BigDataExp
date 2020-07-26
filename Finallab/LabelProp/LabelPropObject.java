import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class LabelPropObject implements WritableComparable<LabelPropObject> {
    public String name = "";
    public int label = 0;
    public double prob = 0;
    public String link = "";

    public LabelPropObject() {
        super();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        label = in.readInt();
        prob = in.readDouble();
        link = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(label);
        out.writeDouble(prob);
        out.writeUTF(link);
    }

    @Override
    public int compareTo(LabelPropObject obj) {
        if (name.equals(obj.name))
            return Integer.compare(label, obj.label);
        else
            return name.compareTo(obj.name);
    }

    @Override
    public String toString() {
        return name + '\t' + label + '\t' + prob + '\t' + link;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
    
    public void fromString1(String str) {
        String[] buffer = str.split("\\s+");
        name = buffer[0];
        label = 0;
        prob = 0.0;
        link = buffer[1];
    }
    public void fromString2(String str) {
        String[] buffer = str.split("\t");
        name = buffer[0];
        label = Integer.parseInt(buffer[1]);
        prob = Double.parseDouble(buffer[2]);
        link = buffer[3];
    }
}