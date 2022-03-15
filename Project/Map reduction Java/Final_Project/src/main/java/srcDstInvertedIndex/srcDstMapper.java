package srcDstInvertedIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class srcDstMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text src = new Text();
    private Text dst = new Text();
    int iteration=0;
    public void map(LongWritable key, Text values, Context context) {

        if (iteration==0) {
            iteration++;
        }

        try {
            String[] tokens = values.toString().split(",");
            src.set(tokens[16]);
            dst.set(tokens[17]);
            if(!tokens[16].equals("NA")) {
                context.write(src, dst);
            }
        } catch (Exception ex) {
            System.out.println("Error in Mapper" + ex.getMessage());
        }
    }
}
