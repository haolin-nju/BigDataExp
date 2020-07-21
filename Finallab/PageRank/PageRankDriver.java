public class PageRankDriver {
    private static int times = 10;// to modify
    public static void main(String args[]) throws Exception {
        String[] forGB = {args[0], args[1]+"/Data0"};
        long row_cnt = GraphBuilder.main(forGB);
        String[] forItr = {"",""};

        /////////////////////////////////////////
        // Better Idea: Iterate until converge //
        /////////////////////////////////////////
        for (int i=0; i<times; i++) {
            forItr[0] = args[1]+"/Data"+i;
            forItr[1] = args[1]+"/Data"+(i+1);
            PageRankIter.main(forItr, row_cnt);
        }

        String[] forRV = {args[1]+"/Data"+times, args[1]+"/FinalRank"};
        PageRankViewer.main(forRV);
    }
}
