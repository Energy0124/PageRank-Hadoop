import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

public class PageRank {
    private static final Logger LOG = Logger.getLogger(PageRank.class);

    public static void main(String[] args) throws Exception {

        //preprocess job
        Configuration preProcessConf = new Configuration();
        Job preProcessJob = Job.getInstance(preProcessConf, "preprocess");
        preProcessJob.setJarByClass(PageRank.class);
        preProcessJob.setMapperClass(PRPreProcess.PreProcessMapper.class);
        //implement combiner later
        //job.setCombinerClass();
        preProcessJob.setReducerClass(PRPreProcess.PreProcessReducer.class);

        preProcessJob.setMapOutputKeyClass(LongWritable.class);
        preProcessJob.setMapOutputValueClass(LongWritable.class);

        preProcessJob.setOutputKeyClass(LongWritable.class);
        preProcessJob.setOutputValueClass(PRNodeWritable.class);

        preProcessJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        preProcessJob.setInputFormatClass(TextInputFormat.class);


        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(preProcessJob, inputPath);

        Path preProcessedOutputPath = new Path("preProcessedOutput-" + UUID.randomUUID());
        FileOutputFormat.setOutputPath(preProcessJob, preProcessedOutputPath);

        if (!preProcessJob.waitForCompletion(true)) {
            System.exit(1);
        }

        long totalNodeCount = preProcessJob.getCounters().findCounter(Counter.TOTAL_NODES).getValue();
        LOG.info("Total nodes: " + totalNodeCount);

        Path pageRankInputPath = preProcessedOutputPath;
        Path pageRankOutputPath = new Path("pageRankOutput-" + UUID.randomUUID());

        int iteration = Integer.parseInt(args[2]);
        double randomJumpFactor = Double.parseDouble(args[3]);
        double threshold = Double.parseDouble(args[4]);

        //pagerank job
        for (int i = 0; i < iteration; i++) {
            Configuration pageRankConf = new Configuration();
            pageRankConf.setLong("totalNodeCount", totalNodeCount);
            Job pageRankJob = Job.getInstance(pageRankConf, "pagerank");
            pageRankJob.setJarByClass(PageRank.class);
            pageRankJob.setMapperClass(PageRankMapper.class);
            //implement combiner later
            //pageRankJob.setCombinerClass();
            pageRankJob.setReducerClass(PageRankReducer.class);

            pageRankJob.setMapOutputKeyClass(LongWritable.class);
            pageRankJob.setMapOutputValueClass(PRNodeWritable.class);

            pageRankJob.setOutputKeyClass(LongWritable.class);
            pageRankJob.setOutputValueClass(PRNodeWritable.class);

            pageRankJob.setInputFormatClass(SequenceFileInputFormat.class);
            pageRankJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            pageRankOutputPath = new Path("pageRankOutput-" + UUID.randomUUID());

            FileInputFormat.addInputPath(pageRankJob, pageRankInputPath);
            FileOutputFormat.setOutputPath(pageRankJob, pageRankOutputPath);

            if (!pageRankJob.waitForCompletion(true)) {
                System.exit(1);
            }


            double missingMass = Double.longBitsToDouble(pageRankJob.getCounters().findCounter(Counter.MISSING_MASS).getValue());


            //run pagerank adjust
            boolean lastRun = false;
            if (i == iteration - 1) {
                lastRun = true;
            }
            Configuration pageRankAdjustConf = new Configuration();
            pageRankAdjustConf.setLong("totalNodeCount", totalNodeCount);
            pageRankAdjustConf.setDouble("missingMass", missingMass);
            pageRankAdjustConf.setDouble("randomJumpFactor", randomJumpFactor);
            pageRankAdjustConf.setDouble("threshold", threshold);
            pageRankAdjustConf.setBoolean("lastRun", lastRun);
            Job pageRankAdjustJob = Job.getInstance(pageRankAdjustConf, "pagerank-adjust");
            pageRankAdjustJob.setJarByClass(PageRank.class);
            pageRankAdjustJob.setMapperClass(PRAdjust.PageRankAdjustMapper.class);
            //implement combiner later
            //pageRankJob.setCombinerClass();
            pageRankAdjustJob.setReducerClass(PRAdjust.PageRankAdjustReducer.class);

            pageRankAdjustJob.setMapOutputKeyClass(LongWritable.class);
            pageRankAdjustJob.setMapOutputValueClass(PRNodeWritable.class);

            pageRankAdjustJob.setOutputKeyClass(LongWritable.class);
            pageRankAdjustJob.setOutputValueClass(PRNodeWritable.class);

            pageRankAdjustJob.setInputFormatClass(SequenceFileInputFormat.class);
            if (lastRun) {
                pageRankAdjustJob.setOutputFormatClass(TextOutputFormat.class);
            } else {
                pageRankAdjustJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            }

            Path pageRankAdjustInputPath = pageRankOutputPath;

            Path pageRankAdjustOutputPath;
            if (lastRun) {
                pageRankAdjustOutputPath = new Path(args[1]);
            } else {
                pageRankAdjustOutputPath = new Path("pageRankAdjustOutput-" + UUID.randomUUID());
            }

            FileInputFormat.addInputPath(pageRankAdjustJob, pageRankAdjustInputPath);
            FileOutputFormat.setOutputPath(pageRankAdjustJob, pageRankAdjustOutputPath);

            if (!pageRankAdjustJob.waitForCompletion(true)) {
                System.exit(1);
            }

            pageRankInputPath = pageRankAdjustOutputPath;

        }

        System.exit(0);


    }

    public enum Counter {
        TOTAL_NODES,
        MISSING_MASS

    }

    public static class PageRankMapper extends
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
                node.setPageRank(1.0D / totalNodeCount);
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
    public static class PageRankReducer extends
            Reducer<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {

        private static final PRNodeWritable node = new PRNodeWritable(PRNodeWritable.Type.Complete);

        @Override
        public void reduce(LongWritable nodeId, Iterable<PRNodeWritable> prNodeWritables, Context context)
                throws IOException, InterruptedException {

            if (nodeId.get() == -1) {
                //if missing mass
                for (PRNodeWritable prNodeWritable : prNodeWritables) {

                    long missingLong = context.getCounter(Counter.MISSING_MASS).getValue();
                    double missingDouble = Double.longBitsToDouble(missingLong);
                    missingDouble += prNodeWritable.getPageRank();
                    missingLong = Double.doubleToLongBits(missingDouble);
                    context.getCounter(Counter.MISSING_MASS).setValue(missingLong);

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
