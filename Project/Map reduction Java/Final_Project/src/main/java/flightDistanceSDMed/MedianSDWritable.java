package flightDistanceSDMed;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianSDWritable implements Writable {
    private float standardDeviation_Distance;
    private float median_Distances;

    public MedianSDWritable(){
        super();
    }

    public MedianSDWritable(float standardDeviation_Distance, float median_Distances) {
        super();
        this.standardDeviation_Distance = standardDeviation_Distance;
        this.median_Distances = median_Distances;
    }

    public float getMedian_Distances() {
        return median_Distances;
    }

    public void setMedian_Distances(float median_Distances) {
        this.median_Distances = median_Distances;
    }

    public float getStandardDeviation_Distance() {
        return standardDeviation_Distance;
    }

    public void setStandardDeviation_Distance(float standardDeviation_Distance) {
        this.standardDeviation_Distance = standardDeviation_Distance;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(standardDeviation_Distance);
        dataOutput.writeFloat(median_Distances);
    }

    public void readFields(DataInput dataInput) throws IOException {
        standardDeviation_Distance=dataInput.readFloat();
        median_Distances=dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "Standard Deviation : "+ standardDeviation_Distance+" --- Median_Distance: " + median_Distances;
    }
}
