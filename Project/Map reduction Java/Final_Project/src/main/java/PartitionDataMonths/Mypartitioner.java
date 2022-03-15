package PartitionDataMonths;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class Mypartitioner extends Partitioner<IntWritable, Text> implements Configurable {

    private static final String MIN_LAST_ACCESS_DATE_MONTH = "min.last.access.date.month";

    private Configuration conf = null;
    private int minLastAccessDateMonth = 0;

    public int getPartition(IntWritable key, Text value, int numPartitions) {
        int a = (key.get() - minLastAccessDateMonth)%numPartitions;
        int b = (key.get() - minLastAccessDateMonth);
        return b;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        minLastAccessDateMonth = conf.getInt(MIN_LAST_ACCESS_DATE_MONTH, 0);
    }

    public static void setMinLastAccessDate(Job job, int minLastAccessDateMonth) {
        job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_MONTH, minLastAccessDateMonth);
    }
}
