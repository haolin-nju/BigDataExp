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
import java.util.*;

public class PageRankConverge {
    // 判断是否收敛
    private static int pr_row_cnt;

    public static class NamePR {
        private String name;
        private double pr;

        public NamePR(String str, double pr_) {
            this.name = str;
            this.pr = pr_;
        }

        public NamePR(NamePR npr) {
            this.name = npr.getName();
            this.pr = npr.getPR();
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

    static Comparator<NamePR> NamePRComparator = new Comparator<NamePR>() {
        @Override
        public int compare(NamePR o1, NamePR o2) {
            // 比较两个对象的pr值
            double pr1 = o1.getPR();
            double pr2 = o2.getPR();
            return pr1 == pr2 ? 0 : pr1 < pr2 ? 1 : -1;
        }
    };

    private static void GetNameList(NamePR[] lines, FileSystem hdfs, FileStatus[] stats, FSDataInputStream in, Scanner scan) throws IOException {
        // 读取pagerank结果，得到人名及对应的pr值
        int cur_idx = 0;
        String[] cur_line;
        for (int i = 0; i < stats.length; i++) {
            in = hdfs.open(stats[i].getPath());
            scan = new Scanner(in);
            while (scan.hasNext()) {
                cur_line = scan.nextLine().split("\\s+");
                lines[cur_idx] = new NamePR(cur_line[0], Double.valueOf(cur_line[1]));
                ++cur_idx;
            }
            scan.close();
            in.close();
        }
    }

    public static String[] topKFrequent(NamePR[] lines, int k) {
        // TopK Algorithm, time complexity: O(NlogK)
        Queue<NamePR> que = new PriorityQueue<>(NamePRComparator);
        String[] result = new String[k];
        for (int i = 0; i < pr_row_cnt; ++i) {
            que.add(lines[i]);
        }
        for (int i = 0; i < k; ++i) {
            result[i] = que.poll().getName();
        }
        return result;
    }

    public static boolean main(String args[], int row_cnt) throws IOException, ClassNotFoundException, InterruptedException {
        pr_row_cnt = row_cnt;
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        NamePR[] lines_old = new NamePR[row_cnt];
        NamePR[] lines_new = new NamePR[row_cnt];
        FSDataInputStream in = null;
        Scanner scan = null;
        try {
            // 迭代前pr值
            FileStatus[] stats = hdfs.listStatus(new Path(args[0]));
            GetNameList(lines_old, hdfs, stats, in, scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            // 迭代后pr值
            FileStatus[] stats = hdfs.listStatus(new Path(args[1]));
            GetNameList(lines_new, hdfs, stats, in, scan);
        } catch (IOException e) {
            e.printStackTrace();
        }

//        Arrays.sort(lines_old, NamePRComparator);
//        Arrays.sort(lines_new, NamePRComparator);
//        for (int i = 0; i < row_cnt; ++i) { // the top row_cnt / 10 is converged
//            if (lines_old[i].getName().equals(lines_new[i].getName()) == false) {
//                return false;
//            }
//        }
        int top_cnt = row_cnt / 5;
        String[] topk_name_old = topKFrequent(lines_old, top_cnt);
        String[] topk_name_new = topKFrequent(lines_new, top_cnt);

        // 判断前N/5个是否相同
        for (int i = 0; i < top_cnt; ++i) {
            if (topk_name_old[i].equals(topk_name_new[i]) == false) {
                return false;
            }
        }

        return true;
    }
}
