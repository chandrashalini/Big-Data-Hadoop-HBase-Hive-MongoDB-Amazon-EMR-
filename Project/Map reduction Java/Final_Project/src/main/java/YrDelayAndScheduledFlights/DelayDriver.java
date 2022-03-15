package YrDelayAndScheduledFlights;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DelayDriver {
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        job.setJarByClass(DelayMapper.class);

        job.setMapperClass(DelayMapper.class);
        //job.setCombinerClass(StockReducer.class);
        job.setReducerClass(DelayReducer.class);
        job.setNumReduceTasks(1);

        TextInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DelayAndSchedule.class);

        FileOutputFormat.setOutputPath(job, outputDir);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DelayAndSchedule.class);


        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}
