package top10srcDstFlights;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class MyReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
    TreeMap<Integer,String> tm = new TreeMap<Integer, String>(Collections.<Integer>reverseOrder());
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count =0;
        for(IntWritable a: values){
            count+=a.get();
        }
        //context.write(key,new IntWritable(count));
        tm.put(count,key.toString());
        if(tm.size()>10)
            tm.remove(tm.lastKey());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry <Integer,String> mentry: tm.entrySet()){
            Text k = new Text(mentry.getValue());
            IntWritable v = new IntWritable(mentry.getKey());
            context.write(k,v);
        }
    }
}
