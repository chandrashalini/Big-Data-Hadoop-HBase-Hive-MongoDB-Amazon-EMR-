package InnerJoinCarrierSrcDst;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class CarrierSrcDstJoinReducer extends Reducer<Text, Text, Text, Text> {

    //private static final Text EMPTY_TEXT = Text("");
    private Text tmp = new Text();
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Get the type of join from our configuration
        joinType = context.getConfiguration().get("join.type");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        // Clear our lists
        listA.clear();
        listB.clear();

        // iterate through all our values, binning each record based on what
        // it was tagged with.  Make sure to remove the tag!

        if(key.toString().equals("PS"))
        {
            System.out.println();
        }

        while (values.iterator().hasNext()) {
            tmp = values.iterator().next();

            if (tmp.charAt(0) == 'S') {
                listA.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.charAt(0) == 'C') {
                listB.add(new Text(tmp.toString().substring(1)));
            }
        }
        executeJoinLogic(context);

    }

    private void executeJoinLogic(Context context)
            throws IOException, InterruptedException {

        if (joinType.equalsIgnoreCase("inner")) {
            // If both lists are not empty, join A with B
            if (!listA.isEmpty() && !listB.isEmpty()) {
                for (Text A : listA) {
                    for (Text B : listB) {
                        context.write(A, B);
                    }
                }
            }
        }
    }
}
