package YrDelayAndScheduledFlights;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayReducer extends Reducer<Text, DelayAndSchedule,Text,DelayAndSchedule> {

    @Override
    protected void reduce(Text key, Iterable<DelayAndSchedule> values, Context context) throws IOException, InterruptedException {
        int delay=0, schedule=0;

        for(DelayAndSchedule a:values){
            delay+=a.getDelayed();
            schedule+=a.getScheduled();
        }
        DelayAndSchedule output = new DelayAndSchedule(schedule,delay);
        context.write(key,output);
    }
}

