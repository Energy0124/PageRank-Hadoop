import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PRAdjust {


    public static class PageRankAdjustMapper extends
            Mapper<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {

        @Override
        public void map(LongWritable nodeId, PRNodeWritable node, Context context)
                throws IOException, InterruptedException {

            long totalNodeCount = context.getConfiguration().getLong("totalNodeCount", 0);
            double randomJumpFactor = context.getConfiguration().getDouble("randomJumpFactor", 0);
            double missingMass = context.getConfiguration().getDouble("missingMass", 0);

            double newPageRank = randomJumpFactor * (1.0D / totalNodeCount) + (1.0D - randomJumpFactor) *
                    (missingMass / totalNodeCount + node.getPageRank());
            node.setPageRank(newPageRank);

            context.write(nodeId, node);


        }
    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    public static class PageRankAdjustReducer extends
            Reducer<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {

        @Override
        public void reduce(LongWritable nodeId, Iterable<PRNodeWritable> prNodeWritables, Context context)
                throws IOException, InterruptedException {
            boolean lastRun = context.getConfiguration().getBoolean("lastRun", false);
            double threshold = context.getConfiguration().getDouble("threshold", 1);

            if (lastRun) {
                for (PRNodeWritable node : prNodeWritables) {
                    if (node.getPageRank() > threshold) {
                        context.write(nodeId, node);
                    }
                }

            } else {
                for (PRNodeWritable node : prNodeWritables) {
                    context.write(nodeId, node);
                }
            }

        }

    }
}
