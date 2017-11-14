import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class PRNodeWritable implements Writable {
    private static final Type[] mapping = new Type[]{Type.Complete, Type.Mass, Type.Structure};
    private Type type = Type.Complete;
    private long nodeID = 0;
    private double pageRank = -1;
    private ArrayList<Long> adjacenyList = new ArrayList<>();

    public PRNodeWritable() {

    }

    public PRNodeWritable(Type type) {
        this.type = type;
    }

    public PRNodeWritable(Type type, long nodeID, double pageRank, ArrayList<Long> adjacenyList) {
        this.type = type;
        this.nodeID = nodeID;
        this.pageRank = pageRank;
        this.adjacenyList = adjacenyList;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(type.val);
        dataOutput.writeLong(nodeID);

        if (type.equals(Type.Mass)) {
            dataOutput.writeDouble(pageRank);
            return;
        }

        if (type.equals(Type.Complete)) {
            dataOutput.writeDouble(pageRank);
        }

        int numValues = adjacenyList.size();
        dataOutput.writeInt(numValues);                 // write number of values
        for (Long value : adjacenyList) {
            dataOutput.writeLong(value);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int b = dataInput.readByte();
        type = mapping[b];
        nodeID = dataInput.readLong();

        if (type.equals(Type.Mass)) {
            pageRank = dataInput.readDouble();
            return;
        }

        if (type.equals(Type.Complete)) {
            pageRank = dataInput.readDouble();
        }

        adjacenyList.clear();
        int numValues = dataInput.readInt();
        adjacenyList.ensureCapacity(numValues);
        for (int i = 0; i < numValues; i++) {
            adjacenyList.add(dataInput.readLong());
        }
    }


    public String toPrettyString() {
        return "PRNodeWritable{" +
                "type=" + type +
                ", nodeID=" + nodeID +
                ", pageRank=" + pageRank +
                ", adjacenyList=" + adjacenyList +
                '}';
    }

    @Override
    public String toString() {
        return Double.toString(pageRank);
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getNodeID() {
        return nodeID;
    }

    public void setNodeID(long nodeID) {
        this.nodeID = nodeID;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public ArrayList<Long> getAdjacenyList() {
        return adjacenyList;
    }

    public void setAdjacenyList(ArrayList<Long> adjacenyList) {
        this.adjacenyList = adjacenyList;
    }

    public enum Type {
        Complete((byte) 0),  // PageRank mass and adjacency list.
        Mass((byte) 1),      // PageRank mass only.
        Structure((byte) 2); // Adjacency list only.

        public byte val;

        Type(byte v) {
            this.val = v;
        }
    }
}
