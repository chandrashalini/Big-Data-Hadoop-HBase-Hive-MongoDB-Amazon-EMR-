package totalOrderSortingDepTime;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class totalOrderMapper extends Mapper<Object, Text, Text, Text> {
    int iteration=0;

    //private Text outkey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);

            if(!data[4].equals("NA")){
                int dep_time = Integer.parseInt(data[4]);
                context.write(new Text(data[4]), new Text(data[8]));
            }
        }
    }
}
