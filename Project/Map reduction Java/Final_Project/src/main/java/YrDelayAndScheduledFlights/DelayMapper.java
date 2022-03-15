package YrDelayAndScheduledFlights;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayMapper extends Mapper<LongWritable, Text, Text, DelayAndSchedule> {
    int iteration = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();

        if (iteration == 0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            int delay=1;
            //System.out.println(data[15]);
            if(data[15].equals("NA"))
                delay=0;
            else if(Integer.parseInt(data[15])==0)
                delay=0;

            DelayAndSchedule dAs = new DelayAndSchedule(1,delay);
            context.write(new Text(data[0]), dAs);
        }
    }
}
