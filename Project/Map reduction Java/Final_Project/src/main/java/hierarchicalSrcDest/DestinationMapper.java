package hierarchicalSrcDest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DestinationMapper extends Mapper<Object, Text, Text, Text> {
    private Text outkey = new Text();
    private Text outvalue = new Text();
    int iteration = 0;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();

        if (iteration == 0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            //S for Source
            outkey.set(data[16]);
            String v = data[17]+" , Carrier := "+data[8];
            outvalue.set("D" + v);
            context.write(outkey, outvalue);
        }
    }
}