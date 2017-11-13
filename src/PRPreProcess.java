import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class PRPreProcess {
    public static class PreProcessMapper
            extends Mapper<Object, Text, LongWritable, LongWritable> {
        private static final LongWritable fromNodeId = new LongWritable();
        private static final LongWritable toNodeId = new LongWritable();
        private static final LongWritable noNodeId = new LongWritable(-1);


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] part = str.split(" ");
            fromNodeId.set(Long.parseLong(part[0]));
            toNodeId.set(Long.parseLong(part[1]));
            context.write(fromNodeId, toNodeId);
            context.write(toNodeId, noNodeId);
        }
    }

    public static class PreProcessReducer
            extends Reducer<LongWritable, LongWritable, LongWritable, PRNodeWritable> {

        private static final PRNodeWritable prNode = new PRNodeWritable(PRNodeWritable.Type.Complete);

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Long> adjList = new ArrayList<>();

            for (LongWritable value : values) {
                if (value.get() >= 0) {
                    adjList.add(value.get());
                }
            }

            prNode.setNodeID(key.get());
            prNode.setPageRank(0.25);
            prNode.setAdjacenyList(adjList);

            context.write(key, prNode);
            context.getCounter(PageRank.Counter.TOTAL_NODES).increment(1);

        }
    }
}
