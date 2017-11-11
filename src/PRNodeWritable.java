import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class PRNodeWritable implements Writable {
    public static enum Type {
        Complete((byte) 0),  // PageRank mass and adjacency list.
        Mass((byte) 1),      // PageRank mass only.
        Structure((byte) 2); // Adjacency list only.

        public byte val;

        private Type(byte v) {
            this.val = v;
        }
    }

    ;
    private long nodeID;
    private double pageRank;
    private ArrayList<Long> adjacenyList;

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

}
