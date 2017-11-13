import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class PRPreProcess {
    public static class PreProcessMapper
            extends Mapper<Object, Text, LongWritable, LongWritable> {
        LongWritable fromNodeId;
        LongWritable toNodeId;


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] part = str.split(" ");
            fromNodeId.set(Long.parseLong(part[0]));
            toNodeId.set(Long.parseLong(part[1]));
            context.write(fromNodeId, toNodeId);
            context.write(toNodeId, fromNodeId);
        }
    }

    public static class PreProcessReducer
            extends Reducer<LongWritable, LongWritable, LongWritable, PRNodeWritable> {
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Long> adjList = new ArrayList<>();

            for (LongWritable value : values) {
                adjList.add(value.get());
            }

            PRNodeWritable pdNode = new PRNodeWritable(PRNodeWritable.Type.Complete, key.get(), 0.25, adjList);
            context.write(key, pdNode);

        }
    }

}
