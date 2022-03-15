package busyAirport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BAirportCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable a:values){
            sum+=a.get();
        }
        Text a = new Text(key.toString()+","+sum);
        context.write(key,new IntWritable(sum));
    }
}
