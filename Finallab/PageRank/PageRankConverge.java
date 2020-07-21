import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Scanner;

public class PageRankConverge {
    public static class NamePR{
        private String name;
        private double pr;
        public NamePR(String str, double pr_){
            this.name = str;
            this.pr = pr_;
        }
        public String getName() {
            return name;
        }
        public double getPR() {
            return pr;
        }
        public void setName(String str) {
            this.name = str;
        }
        public void setPR(int pr_) {
            this.pr = pr_;
        }
    }
    public static class NamePRComparator implements Comparator<NamePR> {
        @Override
        public int compare(NamePR o1, NamePR o2) {
            double pr1 = o1.getPR();
            double pr2 = o2.getPR();
            return pr1 == pr2 ? 0 : pr1 < pr2 ? 1 : -1;
        }
    }
    private static void GetNameList(NamePR[] lines, FileSystem hdfs, FileStatus[] stats, FSDataInputStream in, Scanner scan) throws IOException{
        int cur_idx = 0;
        String[] cur_line;
        for (int i = 0; i < stats.length; i++) {
            in = hdfs.open(stats[i].getPath());
            scan = new Scanner(in);
            while (scan.hasNext()) {
                cur_line = scan.nextLine().split("\\s+");
                lines[cur_idx] = new NamePR(cur_line[0],Double.valueOf(cur_line[1]));
                ++cur_idx;
            }
            scan.close();
            in.close();
        }
    }
    public static boolean main(String args[], int row_cnt) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        NamePR[] lines_old = new NamePR[row_cnt];
        NamePR[] lines_new = new NamePR[row_cnt];
        FSDataInputStream in = null;
        Scanner scan = null;
        try {
            FileStatus[] stats = hdfs.listStatus(new Path(args[0]));
            GetNameList(lines_old, hdfs, stats, in, scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileStatus[] stats = hdfs.listStatus(new Path(args[1]));
            GetNameList(lines_new, hdfs, stats, in, scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Comparator cmp = new NamePRComparator();
        Arrays.sort(lines_old, cmp);
        Arrays.sort(lines_new, cmp);
        for (int i = 0; i < row_cnt / 10; ++i) { // the top row_cnt / 10 is converged
            if (lines_old[i].getName().equals(lines_new[i].getName()) == false) {
                return false;
            }
        }
        return true;
    }
}
