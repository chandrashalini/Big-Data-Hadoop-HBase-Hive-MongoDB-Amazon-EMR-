package airlinehierarchical;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author ajay
 */
public class AirlineHierarchical2 {

    public static class AirlineWritable implements Writable {

        private String airlineState, airlineCity;

        public String getAirlineState() {
            return airlineState;
        }

        public void setAirlineState(String airlineState) {
            this.airlineState = airlineState;
        }

        public String getAirlineCity() {
            return airlineCity;
        }

        public void setAirlineCity(String airlineCity) {
            this.airlineCity = airlineCity;
        }


        public void write(DataOutput d) throws IOException {
            WritableUtils.writeString(d, airlineState);
            WritableUtils.writeString(d, airlineCity);

        }


        public void readFields(DataInput di) throws IOException {
            airlineState = WritableUtils.readString(di);
            airlineCity = WritableUtils.readString(di);

        }

    }

    public static class AirlineHierarchicalMapper extends Mapper<Object, Text, Text, AirlineWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String column[] = value.toString().split(",");
                AirlineWritable ah = new AirlineWritable();

                Text airlineId = new Text(column[0].trim());

                if(column[6].trim().equals("\\N") || column[6].trim().equals("")){
                    return;
                }

                ah.setAirlineState(column[3].trim());
                ah.setAirlineCity(column[2].trim());

                context.write(airlineId, ah);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class AirlineHierarchicalReducer extends Reducer<Text, AirlineWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<AirlineWritable> values, Context context) throws IOException, InterruptedException {

            List<String> airlineCitys = new ArrayList<String>();
            String state = null;

            for (AirlineWritable value : values) {
                state = value.getAirlineState();
                airlineCitys.add(value.getAirlineCity());
            }
            if (!airlineCitys.isEmpty() && state != null) {
                String output = writeXmlFile(state, airlineCitys);
                context.write(new Text(output), NullWritable.get());
            }
        }

        private String transformDocumentToString(Document doc) throws TransformerException {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));

            return writer.getBuffer().toString().replaceAll("\n|\r", "");
        }

        private String writeXmlFile(String state, List<String> list) {

            try {

                DocumentBuilderFactory dFact = DocumentBuilderFactory.newInstance();
                DocumentBuilder build = dFact.newDocumentBuilder();
                Document doc = build.newDocument();

                Element root = doc.createElement("State_City");
                doc.appendChild(root);

                Element stateElement = doc.createElement("state");
                stateElement.appendChild(doc.createTextNode(state));
                root.appendChild(stateElement);

                Element airlinename = doc.createElement("city_names");

                if (!list.isEmpty()) {
                    for (String dtl : list) {
                        Element tag = doc.createElement("city_name");
                        tag.appendChild(doc.createTextNode(dtl));
                        airlinename.appendChild(tag);
                    }
                    root.appendChild(airlinename);
                }

                // Save the document to the disk file
                TransformerFactory tranFactory = TransformerFactory.newInstance();
                Transformer aTransformer = tranFactory.newTransformer();

                // format the XML nicely
                aTransformer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");

                aTransformer.setOutputProperty(
                        "{http://xml.apache.org/xslt}indent-amount", "4");
                aTransformer.setOutputProperty(OutputKeys.INDENT, "yes");

                // location and tags of XML file you can change as per need
                String output = transformDocumentToString(doc);
                return output;

            } catch (TransformerException ex) {
                System.out.println("Error outputting document");

            } catch (ParserConfigurationException ex) {
                System.out.println("Error building document");
            }
            return null;
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AirlineHierarchical");
        job.setJarByClass(AirlineHierarchical2.class);
        job.setMapperClass(AirlineHierarchicalMapper.class);
        job.setReducerClass(AirlineHierarchicalReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AirlineWritable.class);

        //MultipleOutputs.setCountersEnabled(job, true);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
