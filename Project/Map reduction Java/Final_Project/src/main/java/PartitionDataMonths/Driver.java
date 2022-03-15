package PartitionDataMonths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



import java.io.IOException;

public class Driver {

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // Create a new Job
        Job job = Job.getInstance(conf, "DatabyMonth");
        //job.setJarByClass(MyMapper.class);

        // Specify various job-specific parameters
        job.setJobName("DatabyMonth");


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(Mypartitioner.class);
        Mypartitioner.setMinLastAccessDate(job, 1);
        job.setNumReduceTasks(12);


        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path outDir = new Path(args[1]);
        if(fs.exists(outDir)) {
            fs.delete(outDir, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        Configuration conf = new Configuration();
//        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
//        String[] otherArgs = parser.getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: LastAccessDate <in> <out>");
//            ToolRunner.printGenericCommandUsage(System.err);
//            System.exit(2);
//        }
//        Job job = new Job(conf, "LastAccess Date");
//        //job.setJarByClass(LastAccessDate.class);
//        // Set custom partitioner and min last access date
//        job.setPartitionerClass(Mypartitioner.class);
//        Mypartitioner.setMinLastAccessDate(job, 1987);
//        // Last access dates span between 2008-2011, or 4 years
//        job.setNumReduceTasks(1);
//        job.setMapperClass(MyMapper.class);
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setReducerClass(MyReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        //boolean success = job.waitForCompletion(true);
//        //return success ? 0 : 1;
//        FileSystem fs = FileSystem.get(job.getConfiguration());
//        Path outDir = new Path(args[1]);
//        if (fs.exists(outDir)) {
//            fs.delete(outDir, true);
//        }
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
