package top10srcDstFlights;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    int iteration =0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(iteration==0){
            iteration++;
            return;
        }
        else{
            String[]tokens = value.toString().split(",");
            StringBuilder sb = new StringBuilder();
            sb.append(tokens[16]);
            sb.append(",");
            sb.append(tokens[17]);
            Text outValue = new Text(sb.toString());
            context.write(outValue,new IntWritable(1));
        }
    }
}
