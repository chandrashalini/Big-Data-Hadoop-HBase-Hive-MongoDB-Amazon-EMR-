package flightDistanceSDMed;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class DistanceReducer extends Reducer<Text, DistanceWritable,Text,MedianSDWritable> {

    MedianSDWritable msdw = new MedianSDWritable();
    ArrayList<Float> al = new ArrayList<Float>();

    @Override
    protected void reduce(Text key, Iterable<DistanceWritable> values, Context context) throws IOException, InterruptedException {
        float sum =0;
        float count=0;
        al.clear();
        msdw.setStandardDeviation_Distance(0);

        for(DistanceWritable rw : values) {
            sum+=rw.getDistance();
            count+=rw.getCount();
            al.add(rw.getDistance());
        }

        Collections.sort(al);

        if(count%2==0){
            msdw.setMedian_Distances((al.get((int)count/2-1) + al.get((int)count/2))/2.0f);
        }
        else
        {
            msdw.setMedian_Distances(al.get((int)count/2));
        }

        float avg = sum/count;
        float sumOfSquares =0.0f;
        for(float res :al){
            sumOfSquares+= Math.pow(res-avg,2);
        }
        msdw.setStandardDeviation_Distance((float)Math.sqrt(sumOfSquares/(count-1)));
        context.write(key,msdw);

    }
}
