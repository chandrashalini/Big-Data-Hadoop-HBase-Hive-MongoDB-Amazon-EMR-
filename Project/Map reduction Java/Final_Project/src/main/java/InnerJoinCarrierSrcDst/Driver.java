package InnerJoinCarrierSrcDst;

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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Driver {
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();
//        if (otherArgs.length != 4) {
//            printUsage();
//        }
        Job job = new Job(conf, "ReduceSideJoin");
        job.setJarByClass(Driver.class);

        // Use MultipleInputs to set which input uses what mapper
        // This will keep parsing of each data set separate from a logical
        // standpoint
        // The first two elements of the args array are the two inputs
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, SrcDstMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, CarrierMapper.class);
        job.getConfiguration().set("join.type", "inner");

        job.setReducerClass(CarrierSrcDstJoinReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path outDir = new Path(args[2]);
        if(fs.exists(outDir)) {
            fs.delete(outDir, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
