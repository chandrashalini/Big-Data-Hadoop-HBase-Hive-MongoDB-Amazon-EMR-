package worst20SourceAirport;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class topReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    //private TreeMap<Integer, Text> airportData = new TreeMap<Integer, Text>(Collections.reverseOrder());
    private TreeMap<Integer, Map<String,Text>> airportData = new TreeMap<Integer, Map<String, Text>>(Collections.reverseOrder());

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            String line = value.toString();
            String[] data = line.split(",");

            int delay = Integer.parseInt(data[6]);
            //Text val = new Text(data[16]);
            String val2 = data[16];
            boolean check =false;
            Map<String, Text> map = new HashMap<String, Text>();
            map.put(val2, new Text(line));
            if (airportData.size() != 0) {
                for (Map.Entry<Integer,Map<String,Text>> mentry : airportData.entrySet()) {
                    Map<String, Text> map_internal = mentry.getValue();
                    int k = mentry.getKey();
                    if (map_internal.containsKey(val2) && k < delay) {
                        airportData.remove(mentry.getKey());
                        airportData.put(delay,map);
                        check=true;
                        break;
                    }
//                    else{
//                        airportData.put(delay,map);
//                        break;
//                    }
                }
                if(!check) airportData.put(delay,map);
            } else {
                airportData.put(delay, map);

            }
            if (airportData.size() > 20) {
                airportData.remove(airportData.lastKey());
            }
        }


        for(Map.Entry<Integer,Map<String,Text>> mentry:airportData.entrySet()){
            Map<String,Text> map_internal = mentry.getValue();
            for(Map.Entry<String,Text> mentry2:map_internal.entrySet()){
                context.write(NullWritable.get(),new Text(mentry2.getKey()));
                break;
            }
        }


//        for(Text value: values){
//            String data = value.toString();
//            String[] fields = data.split(",");
//            airportData.put(Integer.parseInt(fields[6]), new Text(fields[16]));
//            if(airportData.size()>20){
//                airportData.remove(airportData.lastKey());
//            }
//        }
//        for(Map.Entry<Integer,Text> mentry :airportData.entrySet()){
//            context.write(NullWritable.get(),mentry.getValue());
//        }
    }
}
