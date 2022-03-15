package PartitionDataMonths;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

    int iteration = 0;
    //private IntWritable outkey = new IntWritable();

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            // Write out the month with the input value
            int month = Integer.parseInt(data[1]);
            context.write(new IntWritable(month), value);
        }
    }
}
