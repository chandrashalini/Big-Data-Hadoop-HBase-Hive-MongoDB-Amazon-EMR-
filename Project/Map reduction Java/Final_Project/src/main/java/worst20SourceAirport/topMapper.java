package worst20SourceAirport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class topMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    //private TreeMap<Integer,Text> airportData;
    //int iteration =0;
    int iteration =0;
    private TreeMap<Integer,Map<String,Text>> airportData;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        airportData = new TreeMap<Integer,Map<String, Text>>(Collections.reverseOrder());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            if(data[6].equals("NA"))
                return;

            int delay = Integer.parseInt(data[6]);
            //Text val = new Text(data[16]);
            String val2 = data[16];
            boolean check = false;
            Map<String,Text> map = new HashMap<String, Text>();
            map.put(val2,new Text(line));
            if(airportData.size()!=0){
                for(Map.Entry mentry:airportData.entrySet()){
                    Map<String,Text> map_internal = (Map<String, Text>) mentry.getValue();
                    int k = (Integer) mentry.getKey();
                    if(map_internal.containsKey(val2)&&k< delay) {
                        airportData.remove(mentry.getKey());
                        airportData.put(delay,map);
                        check = true;
                        break;
                    }
                }
                if(!check) airportData.put(delay,map);
            }
            else
            {
                airportData.put(delay,map);
            }

            //airportData.put(delay,new Text(line));

            if(airportData.size()>20){
                airportData.remove(airportData.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //super.cleanup(context);

        for(Map.Entry<Integer,Map<String,Text>> mentry:airportData.entrySet()){
            Map<String,Text> map_internal = mentry.getValue();
            for(Map.Entry<String,Text> mentry2:map_internal.entrySet()){
                context.write(NullWritable.get(),mentry2.getValue());
            }
        }

//        for(Map.Entry<Integer,Text> entry: airportData.entrySet()){
//            context.write(NullWritable.get(),entry.getValue());
//        }
    }

//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        //super.setup(context);
//        airportData = new TreeMap<Integer, Text>(Collections.reverseOrder());
//    }
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        //super.map(key, value, context);
//        String cvsSplitBy = ",", line = value.toString();
//
//        if(iteration==0) {
//            iteration++;
//        } else {
//            String[] data = line.split(cvsSplitBy);
//            if(data[6].equals("NA"))
//                return;
//
//            int delay = Integer.parseInt(data[6]);
//            Text val = new Text(data[16]);
//            airportData.put(delay,new Text(line));
//
//            if(airportData.size()>20){
//                airportData.remove(airportData.lastKey());
//            }
//        }
//    }
//
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        //super.cleanup(context);
//        for(Map.Entry<Integer,Text> entry: airportData.entrySet()){
//            context.write(NullWritable.get(),entry.getValue());
//        }
//    }
}
