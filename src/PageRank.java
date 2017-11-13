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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

import org.apache.log4j.Logger;

public class PageRank {
    private static final Logger LOG = Logger.getLogger(PageRank.class);

    public enum Counter {
        TOTAL_NODES,

    }

    private static class PageRankMapper extends
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
            }
        }
    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    private static class PageRankReducer extends
            Reducer<LongWritable, PRNodeWritable, LongWritable, PRNodeWritable> {
        // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
        // through dangling nodes.
        private float totalMass = Float.NEGATIVE_INFINITY;

        @Override
        public void reduce(LongWritable nid, Iterable<PRNodeWritable> iterable, Context context)
                throws IOException, InterruptedException {
            Iterator<PRNodeWritable> values = iterable.iterator();

            // Create the node structure that we're going to assemble back together from shuffled pieces.
            PRNodeWritable node = new PRNodeWritable(PRNodeWritable.Type.Complete);

            node.setNodeID(nid.get());

            int massMessagesReceived = 0;
            int structureReceived = 0;
            double pageRank = 0;
            for (PRNodeWritable prNodeWritable : iterable) {

                if (prNodeWritable.getType().equals(PRNodeWritable.Type.Structure)) {
                    // This is the structure; update accordingly.
                    ArrayList<Long> list = prNodeWritable.getAdjacenyList();
                    structureReceived++;
                    node.setAdjacenyList(list);
                } else {
                    // This is a message that contains PageRank mass; accumulate.
                    pageRank += prNodeWritable.getPageRank();
                    massMessagesReceived++;
                }
            }

            //todo: continue here and think about how to handle missing mass

            // Update the final accumulated PageRank mass.
            node.setPageRank(pageRank);
//            context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

            // Error checking.
            if (structureReceived == 1) {
                // Everything checks out, emit final node structure with updated PageRank value.
                context.write(nid, node);

                // Keep track of total PageRank mass.
//                totalMass = sumLogProbs(totalMass, mass);
            } else if (structureReceived == 0) {
                // We get into this situation if there exists an edge pointing to a node which has no
                // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
                // log and count but move on.
//                context.getCounter(PageRank.missingStructure).increment(1);
                LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
                        + massMessagesReceived);
                // It's important to note that we don't add the PageRank mass to total... if PageRank mass
                // was sent to a non-existent node, it should simply vanish.
            } else {
                // This shouldn't happen!
                throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
                        + " mass: " + massMessagesReceived + " struct: " + structureReceived);
            }
        }

//        @Override
//        public void cleanup(Context context) throws IOException {
//            Configuration conf = context.getConfiguration();
//            String taskId = conf.get("mapred.task.id");
//            String path = conf.get("PageRankMassPath");
//
//            Preconditions.checkNotNull(taskId);
//            Preconditions.checkNotNull(path);
//
//            // Write to a file the amount of PageRank mass we've seen in this reducer.
//            FileSystem fs = FileSystem.get(context.getConfiguration());
//            FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
//            out.writeFloat(totalMass);
//            out.close();
//        }
    }

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
        //pagerank job
        for (int i = 0; i < iteration; i++) {
            Configuration pageRankConf = new Configuration();
            Job pageRankJob = Job.getInstance(pageRankConf, "pagerank");
            pageRankJob.setJarByClass(PageRank.class);
            pageRankJob.setMapperClass(PRPreProcess.PreProcessMapper.class);
            //implement combiner later
            //pageRankJob.setCombinerClass();
            pageRankJob.setReducerClass(PRPreProcess.PreProcessReducer.class);

            pageRankJob.setMapOutputKeyClass(LongWritable.class);
            pageRankJob.setMapOutputValueClass(LongWritable.class);

            pageRankJob.setOutputKeyClass(LongWritable.class);
            pageRankJob.setOutputValueClass(PRNodeWritable.class);

            pageRankJob.setInputFormatClass(SequenceFileInputFormat.class);
            pageRankJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            pageRankOutputPath = new Path("pageRankOutput-" + UUID.randomUUID());

            FileInputFormat.addInputPath(pageRankJob, pageRankInputPath);
            FileOutputFormat.setOutputPath(pageRankJob, pageRankOutputPath);

            if (!preProcessJob.waitForCompletion(true)) {
                System.exit(1);
            }

            //run pagerank adjust
            Configuration pageRankAdjustConf = new Configuration();
            pageRankAdjustConf.setLong("totalNodeCount", totalNodeCount);
            Job pageRankAdjustJob = Job.getInstance(pageRankAdjustConf, "pagerank-adjust");
            pageRankAdjustJob.setJarByClass(PageRank.class);
            pageRankAdjustJob.setMapperClass(PRPreProcess.PreProcessMapper.class);
            //implement combiner later
            //pageRankJob.setCombinerClass();
            pageRankAdjustJob.setReducerClass(PRPreProcess.PreProcessReducer.class);

            pageRankAdjustJob.setMapOutputKeyClass(LongWritable.class);
            pageRankAdjustJob.setMapOutputValueClass(LongWritable.class);

            pageRankAdjustJob.setOutputKeyClass(LongWritable.class);
            pageRankAdjustJob.setOutputValueClass(PRNodeWritable.class);

            pageRankAdjustJob.setInputFormatClass(SequenceFileInputFormat.class);
            pageRankAdjustJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            Path pageRankAdjustInputPath = pageRankOutputPath;
            Path pageRankAdjustOutputPath = new Path("pageRankAdjustOutput-" + UUID.randomUUID());

            FileInputFormat.addInputPath(pageRankAdjustJob, pageRankAdjustInputPath);
            FileOutputFormat.setOutputPath(pageRankAdjustJob, pageRankAdjustOutputPath);

            if (!preProcessJob.waitForCompletion(true)) {
                System.exit(1);
            }

            pageRankInputPath = pageRankAdjustOutputPath;

        }

        System.exit(0);


    }
}
