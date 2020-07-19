public class PageRankDriver {
    private static int times = 1;// to modify
    public static void main(String args[]) throws Exception {
        String[] forGB = {args[0], args[1]+"/Data0"};
        GraphBuilder.build(forGB);
        String[] forItr = {"",""};

        // Better Idea: Iterate until converge
        for (int i=0; i<times; i++) {
            forItr[0] = args[1]+"/Data"+i;
            forItr[1] = args[1]+"/Data"+(i+1);
            PageRankIter.iter(forItr);
        }

        String[] forRV = {args[1]+"/Data"+times, args[1]+"/FinalRank"};
        PageRankViewer.view(forRV);
    }
}
