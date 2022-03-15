package busyAirport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BAirportMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    int iteration = 0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            String origin = data[16],cancelled = data[21];
            if(!cancelled.equals("0")){
                return;
            }
            context.write(new Text(origin),new IntWritable(1));
        }
    }
}
