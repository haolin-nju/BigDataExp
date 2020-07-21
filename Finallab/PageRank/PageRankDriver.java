public class PageRankDriver {
    private static int times = 100;// to modify

    public static void main(String args[]) throws Exception {
        String[] forGB = {args[0], args[1] + "/Data0"};
        int row_cnt = GraphBuilder.main(forGB);
        String[] forItr = {"", ""};

        /////////////////////////////////////////
        // Better Idea: Iterate until converge //
        /////////////////////////////////////////
        int i = 0;
        for (; i < times; i++) {
            forItr[0] = args[1] + "/Data" + i;
            forItr[1] = args[1] + "/Data" + (i + 1);
            PageRankIter.main(forItr, row_cnt, i + 1);
            // We recognize that if the top row_cnt / 10 people are converged, this iteration is converged.
            if (PageRankConverge.main(forItr, row_cnt)) {
                System.out.printf("Interation times: %d\n", i);
                break;
            }
        }
        if (i == times) {
            System.out.printf("Iteration is not converged yet. Iteration times: %d\n", times);
        }

        String[] forRV = {args[1] + "/Data" + (i + 1), args[1] + "/FinalRank"};
        PageRankViewer.main(forRV);
    }
}
