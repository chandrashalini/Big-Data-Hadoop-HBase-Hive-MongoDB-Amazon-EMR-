package BloomFilterSourceDestTest;

import com.google.common.hash.BloomFilter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;

public class BloomFilterMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private BloomFilter<SourceDestination> srcDest;
    int iteration =0;
    Funnel<SourceDestination> sd = new Funnel<SourceDestination>(){

        public void funnel(SourceDestination sourceDestination, Sink sink) {
            sink.putString(sourceDestination.source, Charsets.UTF_8).putString(sourceDestination.destination, Charsets.UTF_8);
        }
    };

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //super.setup(context);
        this.srcDest = BloomFilter.create(sd,50,0.3);
        SourceDestination sdList1 = new SourceDestination("SFO","PDX");
        SourceDestination sdList2 = new SourceDestination("LAS","JFK");

        ArrayList<SourceDestination> sdList = new ArrayList<SourceDestination>();
        sdList.add(sdList1);
        sdList.add(sdList2);

        for(SourceDestination s:sdList){
            srcDest.put(s);
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String cvsSplitBy = ",", line = value.toString();

        if(iteration==0) {
            iteration++;
        } else {
            String[] data = line.split(cvsSplitBy);
            SourceDestination s = new SourceDestination(data[16],data[17]);
            if(srcDest.mightContain(s)){
                context.write(new Text(s.source +"-->"+s.destination),NullWritable.get());
            }

        }

    }
}
