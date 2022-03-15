package busyAirport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BAirportReducer extends Reducer<Text, IntWritable,Text,Text> {
    int max_sum =0;
    HashMap<String,IntWritable> hm;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        hm = new HashMap<String, IntWritable>();
    }


    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum2=0;
        String org2 ="";
        for(IntWritable t:values){
            int sum = t.get();
            sum2+=sum;
        }
        hm.put(key.toString(),new IntWritable(sum2));
        max_sum+=sum2;

        //context.write(key,new Text(sum2+"--->"+max_sum));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry <String,IntWritable> mentry: hm.entrySet()){
            int v = mentry.getValue().get();
            float percent = (float) v/max_sum;
            percent = percent*100;
            context.write(new Text(mentry.getKey()),new Text(mentry.getValue()+"--->"+max_sum+" -->"+
                    percent+"% Busy"));
        }
    }
}
