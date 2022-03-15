package totalDataByYear;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    int iteration = 0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            context.write(new Text(data[0]),new IntWritable(1));
        }
    }
}
