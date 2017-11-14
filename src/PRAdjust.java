import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class PRAdjust {


    public static class PageRankAdjustMapper extends
            Mapper<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {

        // The neighbor to which we're sending messages.
        private static final LongWritable neighborNodeId = new LongWritable();

        // Contents of the messages: partial PageRank mass.
        private static final PRNodeWritable intermediateMass = new PRNodeWritable(PRNodeWritable.Type.Mass);

        // For passing along node structure.
        private static final PRNodeWritable intermediateStructure = new PRNodeWritable(PRNodeWritable.Type.Structure);

        @Override
        public void map(LongWritable nodeId, PRNodeWritable node, Context context)
                throws IOException, InterruptedException {

            if (node.getPageRank() == -1) {
                long totalNodeCount = context.getConfiguration().getLong("totalNodeCount", 0);
                node.setPageRank(1 / totalNodeCount);
            }

            // Pass along node structure.
            intermediateStructure.setNodeID(node.getNodeID());
            intermediateStructure.setType(PRNodeWritable.Type.Structure);
            intermediateStructure.setAdjacenyList(node.getAdjacenyList());

            context.write(nodeId, intermediateStructure);

            // Distribute PageRank mass to neighbors (along outgoing edges).
            if (node.getAdjacenyList().size() > 0) {
                // Each neighbor gets an equal share of PageRank mass.
                ArrayList<Long> adjacenyList = node.getAdjacenyList();
                double mass = node.getPageRank() / adjacenyList.size();

                // Iterate over neighbors.
                for (Long neighborId : adjacenyList) {
                    neighborNodeId.set(neighborId);

                    intermediateMass.setNodeID(neighborId);
                    intermediateMass.setType(PRNodeWritable.Type.Mass);
                    intermediateMass.setPageRank(mass);

                    context.write(neighborNodeId, intermediateMass);
                }
            } else {
                //missing mass
                neighborNodeId.set(-1);

                intermediateMass.setNodeID(-1);
                intermediateMass.setType(PRNodeWritable.Type.Mass);
                intermediateMass.setPageRank(node.getPageRank());

                context.write(neighborNodeId, intermediateMass);

            }
        }
    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    public static class PageRankAdjustReducer extends
            Reducer<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {

        private static final PRNodeWritable node = new PRNodeWritable(PRNodeWritable.Type.Complete);

        @Override
        public void reduce(LongWritable nodeId, Iterable<PRNodeWritable> prNodeWritables, Context context)
                throws IOException, InterruptedException {

            if (nodeId.get() == -1) {
                //if missing mass
                for (PRNodeWritable prNodeWritable : prNodeWritables) {

                    long missingLong = context.getCounter(PageRank.Counter.MISSING_MASS).getValue();
                    double missingDouble = Double.longBitsToDouble(missingLong);
                    missingDouble += prNodeWritable.getPageRank();
                    missingLong = Double.doubleToLongBits(missingDouble);
                    context.getCounter(PageRank.Counter.MISSING_MASS).setValue(missingLong);

                }
                return;
            }

            // Create the node structure that we're going to assemble back together from shuffled pieces.

            node.setNodeID(nodeId.get());

            double pageRank = 0;
            for (PRNodeWritable prNodeWritable : prNodeWritables) {

                if (prNodeWritable.getType().equals(PRNodeWritable.Type.Structure)) {
                    // This is the structure; update accordingly.
                    ArrayList<Long> list = prNodeWritable.getAdjacenyList();
                    node.setAdjacenyList(list);
                } else {
                    // This is a message that contains PageRank mass; accumulate.
                    pageRank += prNodeWritable.getPageRank();
                }
            }

            // Update the final accumulated PageRank mass.
            node.setPageRank(pageRank);
//            context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);
            context.write(nodeId, node);
        }

    }
}
