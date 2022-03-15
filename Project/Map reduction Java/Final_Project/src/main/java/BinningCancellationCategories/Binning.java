package BinningCancellationCategories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Binning {

    /**
     * @param args the command line arguments
     */
    //int iteration =0;
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Binning Cancellation");
        job.setJarByClass(Binning.class);

        //Setting Mapper Class and the output key and value
        job.setMapperClass(BinningMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //No combiner, partitioner or reducer is used in this pattern!
        job.setNumReduceTasks(0);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);
        Path outDir = new Path(args[1]);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(outDir)) {
            fs.delete(outDir, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class BinningMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> mos = null;

        protected void setup(Context context) {
            mos = new MultipleOutputs(context);

        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (key.get() == 0) {
                return;
            }
            String[] token = value.toString().split(",");

            //for(String rate: token){

            String code = token[22].trim();

            if (code.equals("A")) {
                mos.write("bins", value, NullWritable.get(), "Carrier Cancellation Code");
            }
            if (code.equals("B")) {
                mos.write("bins", value, NullWritable.get(), "Weather Cancellation Code");
            }
            if (code.equals("C")) {
                mos.write("bins", value, NullWritable.get(), "NAS Cancellation Code");
            }
            if (code.equals("D")) {
                mos.write("bins", value, NullWritable.get(), "Security Cancellation Code");
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

    }

}

