package hierarchicalSrcDest;

//import com.sun.jersey.spi.StringReader;
//import jdk.internal.org.xml.sax.InputSource;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.w3c.dom.Attr;
//import org.w3c.dom.Document;
//import org.w3c.dom.Element;
//import org.w3c.dom.NamedNodeMap;
//import org.xml.sax.SAXException;
//
//import javax.xml.parsers.DocumentBuilder;
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.parsers.ParserConfigurationException;
//import javax.xml.transform.*;
//import javax.xml.transform.dom.DOMSource;
//import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class SrcDestHierarchyReducer extends Reducer<Text, Text, Text, NullWritable> {
    private ArrayList<String> destination = new ArrayList<String>();
    private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    private String source = null;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Reset variables
        source = null;
        destination.clear();

        // For each input value
        for (Text t : values) {
            // If this is the source record, store it, minus the flag
            if (t.charAt(0) == 'S') {
                source = t.toString().substring(1, t.toString().length())
                        .trim();
            } else {
                // Else, it is a destination record. Add it to the list, minus
                // the flag

                destination.add(t.toString()
                        .substring(1, t.toString().length()).trim());
            }
        }
        // If there are no destination, the destination list will simply be empty.

        // If post is not null, combine source with its destination.
        if (source != null) {
            // nest the comments underneath the post element
            String sourceWithDestinationChildren = null;
            try {
                sourceWithDestinationChildren = nestElements(source, destination);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (TransformerException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            }

            // write out the XML
            context.write(new Text(sourceWithDestinationChildren),
                    NullWritable.get());
        }

    }

    private String nestElements(String source, List<String> destination) throws ParserConfigurationException, TransformerException, IOException, SAXException {
        // Create the new document to build the XML
        DocumentBuilder bldr = dbf.newDocumentBuilder();
        Document doc = bldr.newDocument();

        // Copy parent node to document
        Element sourceEl = getXmlElementFromString(source);
        Element toAddSourceEl = doc.createElement("source");

        // Copy the attributes of the original source element to the new one
        copyAttributesToElement(sourceEl.getAttributes(), toAddSourceEl);

        // For each destination, copy it to the "source" node
        for (String destinationXml : destination) {
            Element destinationEl = getXmlElementFromString(destinationXml);
            Element toAddDestinationEl = doc.createElement("comments");

            // Copy the attributes of the original destination element to
            // the new one
            copyAttributesToElement(destinationEl.getAttributes(), toAddDestinationEl);

            // Add the copied comment to the post element
            toAddSourceEl.appendChild(toAddDestinationEl);
        }

        // Add the post element to the document
        doc.appendChild(toAddSourceEl);

        // Transform the document into a String of XML and return
        return transformDocumentToString(doc);
    }

    private Element getXmlElementFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        // Create a new document builder
        DocumentBuilder bldr = dbf.newDocumentBuilder();

        return bldr.parse(String.valueOf(new InputSource(new StringReader(xml))))
                .getDocumentElement();
    }

    private void copyAttributesToElement(NamedNodeMap attributes,
                                         Element element) {

        // For each attribute, copy it to the element
        for (int i = 0; i < attributes.getLength(); ++i) {
            Attr toCopy = (Attr) attributes.item(i);
            element.setAttribute(toCopy.getName(), toCopy.getValue());
        }
    }

    private String transformDocumentToString(Document doc) throws TransformerException {

        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
                "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(
                writer));
        // Replace all new line characters with an empty string to have
        // one record per line.
        return writer.getBuffer().toString().replaceAll("\n|\r", "");
    }
}

