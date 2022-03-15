package InnerJoinCarrierSrcDst;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SrcDstMapper extends Mapper<LongWritable, Text, Text, Text> {
    int iteration = 0;
    private Text outvalue = new Text();
    private Text outKey = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            StringBuilder sb = new StringBuilder();
            sb.append("S");
            sb.append("-->");
            sb.append(data[16]);
            sb.append("-->");
            sb.append(data[17]);
            sb.append("-->");
            sb.append(data[18]);
            outvalue.set(sb.toString());

            outKey.set(data[8]);
            context.write(outKey,outvalue);
        }
    }
}
