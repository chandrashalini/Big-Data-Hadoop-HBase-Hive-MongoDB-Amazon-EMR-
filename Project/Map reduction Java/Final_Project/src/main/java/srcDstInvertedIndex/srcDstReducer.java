package srcDstInvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class srcDstReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();
    HashSet<String> hs = new HashSet<String>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        boolean first = true;

        for (Text dst : values)
            hs.add(dst.toString());


        for (String dst : hs) {
            if (first){
                first = false;
                sb.append("--->");
            }
            else
                sb.append("  ,");

            sb.append(dst);
        }
        result.set(sb.toString());
        context.write(key, result);
    }
}
