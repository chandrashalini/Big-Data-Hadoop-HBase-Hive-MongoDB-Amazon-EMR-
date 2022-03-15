package flightDistanceSDMed;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistanceWritable implements Writable {
    private float distance;
    private long count;

    public DistanceWritable() {
        super();
    }

    public DistanceWritable(int distance, long count) {
        super();
        this.distance = distance;
        this.count = count;
    }

    public float getDistance() {
        return distance;
    }

    public void setDistance(float distance) {
        this.distance = distance;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeFloat(distance);
    }


    public void readFields(DataInput dataInput) throws IOException {
        count=dataInput.readLong();
        distance=dataInput.readFloat();
    }
    @Override
    public String toString(){
        return "Ratings: "+ distance +" Count: "+count;
    }
}
