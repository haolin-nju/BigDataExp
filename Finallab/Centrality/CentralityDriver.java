import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hsqldb.lib.Sort;

import java.io.IOException;
import java.util.Scanner;

public class CentralityDriver {
    private static int getNodes(String path) throws IOException {
        Configuration conf = new Configuration();
        //ref Book P88-89
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        int row_cnt = 0;
        String str;
        FSDataInputStream in = null;
        Scanner scan;
        try {
            FileStatus[] stats = hdfs.listStatus(new Path(path));
            for (int i = 0; i < stats.length; i++) {
                in = hdfs.open(stats[i].getPath());
                scan = new Scanner(in);
                while (scan.hasNext()) {
                    str = scan.nextLine();
                    row_cnt++;
                }
                scan.close();
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return row_cnt;
    }
    private static int getSum(String path) throws IOException {
        Configuration conf = new Configuration();
        //ref Book P88-89
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        int sum = 0;
        String str;
        FSDataInputStream in = null;
        Scanner scan;
        try {
            FileStatus[] stats = hdfs.listStatus(new Path(path));
            for (int i = 0; i < stats.length; i++) {
                in = hdfs.open(stats[i].getPath());
                scan = new Scanner(in);
                while (scan.hasNext()) {
                    str = scan.nextLine();
                    sum += Integer.valueOf(str.split("\\s+")[1]);
                }
                scan.close();
                in.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sum;
    }

    public static void main(String args[]) throws Exception {
        // args0: input of <aa,bb> c; args1: input of rows to count nodes; args2: output_path
        String[] forDC = {args[0], args[2] + "/DegreeCentral_Output"};
        String[] forEVC = {args[0], args[2] + "/EigenVecCentral_Output"};
        String[] SortforDC = {args[2] + "/DegreeCentral_Output", args[2] + "/Sorted_DegreeCentral_Output"};
        String[] SortforEVC = {args[2] + "/EigenVecCentral_Output", args[2] + "/Sorted_EigenVecCentral_Output"};
        int row_cnt = getNodes(args[1]);
        int sum = getSum(args[0]);
        Integer[] int_arr = new Integer[]{row_cnt, sum};
        System.out.println(int_arr[0]);
        System.out.println(int_arr[1]);
        DegreeCentrality.main(forDC, int_arr[1]);
        EigenVecCentrality.main(forEVC, int_arr);
        SortCentrality.main(SortforDC, "Degree");
        SortCentrality.main(SortforEVC, "EigenVector");
    }
}
