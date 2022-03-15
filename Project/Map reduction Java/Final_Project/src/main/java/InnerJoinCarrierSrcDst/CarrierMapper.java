package InnerJoinCarrierSrcDst;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CarrierMapper extends Mapper<LongWritable, Text, Text, Text> {
    int iteration = 0;
    private Text outvalue = new Text();

    private Text outKey = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();
        if(iteration==0) {
            iteration++;
        } else {
            line = line.replace("\"", "");
            String[] data = line.split(cvsSplitBy);
            StringBuilder sb = new StringBuilder();
            sb.append("C");
            sb.append("-->");
            sb.append(data[1]);
            outvalue.set(sb.toString());
            outKey.set(data[0]);
            if(data[0].equals("PS"))
            {
                System.out.println();
            }
            context.write(outKey,outvalue);
        }
    }
}
