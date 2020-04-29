import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class Order implements WritableComparable<Order> {
    private int oid, odate, pid;
    private String pname = "";
    private int price, oamount;

    // 没有默认构造函数会报错
    public Order() {
        super();
    }

    // 根据flag和buffer初始化相应数据，其余数据使用默认值0
    public Order(String[] buffer, boolean flag) {
        super();
        if (flag) {
            this.oid = Integer.parseInt(buffer[0]);
            this.odate = Integer.parseInt(buffer[1]);
            this.pid = Integer.parseInt(buffer[2]);
            this.oamount = Integer.parseInt(buffer[3]);
        } else {
            this.pid = Integer.parseInt(buffer[0]);
            this.pname = buffer[1];
            this.price = Integer.parseInt(buffer[2]);
        }
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        oid = arg0.readInt();
        odate = arg0.readInt();
        pid = arg0.readInt();
        pname = arg0.readUTF();
        price = arg0.readInt();
        oamount = arg0.readInt();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeInt(oid);
        arg0.writeInt(odate);
        arg0.writeInt(pid);
        arg0.writeUTF(pname);
        arg0.writeInt(price);
        arg0.writeInt(oamount);
    }

    // 注意这里与课上方法不同！如果pid和pname都相等，那么说明数据来自order.txt，此时比较oid
    @Override
    public int compareTo(Order o) {
        return pid == o.pid ? (o.pname.equals(pname) ? oid - o.oid : o.pname.compareTo(pname)) : pid - o.pid;
    }

    // 重写该函数，使得文本输出用空格分隔
    @Override
    public String toString() {
        return oid + " " + odate + " " + pid + " " + pname + " " + price + " " + oamount;
    }

    public int getPid() {
        return pid;
    }

    public String getPname() {
        return pname;
    }

    public int getPrice() {
        return price;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setPrice(int price) {
        this.price = price;
    }
}