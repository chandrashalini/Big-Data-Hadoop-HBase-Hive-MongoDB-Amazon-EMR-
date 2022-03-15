package hierarchicalSrcDest;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "PostCommentHierarchy");
        //job.setJarByClass(BuildingDriver.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, SourceMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, DestinationMapper.class);

        job.setReducerClass(SrcDestHierarchyReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outDir = new Path(args[1]);
//        FileSystem fs = FileSystem.get(job.getConfiguration());
//        if(fs.exists(outDir)) {
//            fs.delete(outDir, true);
//        }

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
