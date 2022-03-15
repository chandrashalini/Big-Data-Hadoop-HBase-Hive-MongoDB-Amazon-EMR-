package BloomFilterSourceDestTest;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        Job job = Job.getInstance();
        job.setJarByClass(Driver.class);
        job.setMapperClass(BloomFilterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        FileSystem hdfs = FileSystem.get(job.getConfiguration());
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        job.waitForCompletion(true);

    }

}
