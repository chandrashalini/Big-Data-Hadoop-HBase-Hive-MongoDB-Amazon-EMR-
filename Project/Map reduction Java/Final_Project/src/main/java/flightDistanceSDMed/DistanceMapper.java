package flightDistanceSDMed;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistanceMapper extends Mapper <LongWritable, Text, Text, DistanceWritable> {
    int iteration = 0;
    //private IntWritable distance = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            if(data[18].equals("NA")){
                return;
            }
            DistanceWritable dw = new DistanceWritable(Integer.parseInt(data[18]),1);
            context.write(new Text(data[0]),dw);
        }
    }
}
