package hierarchicalSrcDest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SourceMapper extends Mapper<Object, Text, Text, Text> {
    private Text outkey = new Text();
    private Text outvalue = new Text();
    int iteration=0;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            //S for Source
            outkey.set(data[16]);
            outvalue.set("S"+line);
            context.write(outkey,outvalue);
        }
    }
}
