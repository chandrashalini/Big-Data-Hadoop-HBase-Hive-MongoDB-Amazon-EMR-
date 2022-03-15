package monthCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class monthDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Counting With Counters");
        job.setJarByClass(monthDriver.class);
        job.setMapperClass(monthMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Path out = new Path(args[1]);
        FileSystem.get(conf).delete(out, true);
        int code = job.waitForCompletion(true) ? 0 : 1;


        SortedMap<Long, String> tm = new TreeMap<Long,String>(new Comparator<Long>() {
            public int compare(Long s1, Long s2) {
                return s2.compareTo(s1);
            }
        });

        if (code == 0) {
            for (Counter counter : job.getCounters().getGroup(monthMapper.MONTH_COUNTER_GROUP)) {
                {
                    //System.out.println("Month : " + counter.getDisplayName() + " has " + counter.getValue() + " number of visits");
                    tm.put(counter.getValue(),counter.getDisplayName());
                }
            }
            for(Map.Entry mentry:tm.entrySet()){
                System.out.println("Month : " + mentry.getValue() + " has " + mentry.getKey() + " number of visits");
            }

        }
        System.exit(code);
    }

}
