package monthCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;

public class monthMapper extends Mapper<Object, Text, NullWritable, NullWritable> {
    private final IntWritable one = new IntWritable(1);
    public static final String MONTH_COUNTER_GROUP = "Month";
    public static final String UNKNOWN_COUNTER = "Unknown";
    public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";

    private String[] monthArray = new String[]{
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Nov", "Oct", "Dec"
    };
    private HashSet<String> months = new HashSet<String>(Arrays.asList(monthArray));

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");
        try {

            int mon = Integer.parseInt(tokens[1]);

            if (mon != 0) {
                String monthName = monthArray[mon - 1];
                boolean unknown = true;

                if (months.contains(monthName)) {
                    context.getCounter(MONTH_COUNTER_GROUP, monthName).increment(1);
                    unknown = false;
                }

                if (unknown) {
                    context.getCounter(UNKNOWN_COUNTER, UNKNOWN_COUNTER).increment(1);
                }
            } else {
                context.getCounter(NULL_OR_EMPTY_COUNTER, NULL_OR_EMPTY_COUNTER).increment(1);
            }
        } catch (Exception e) {
            e.getMessage();
        }
    }
}

