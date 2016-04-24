import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class QFDMatcherReducer extends Reducer<IntWritable, WebTrafficRecord, RequestReplyMatch, NullWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<WebTrafficRecord> values,
                       Context ctxt) throws IOException, InterruptedException {

        // The input is a set of WebTrafficRecords for each key,
        // the output should be the WebTrafficRecords for
        // all cases where the request and reply are matched
        // as having the same
        // Source IP/Source Port/Destination IP/Destination Port
        // and have occured within a 10 second window on the timestamp.

        // One thing to really remember, the Iterable element passed
        // from hadoop are designed as READ ONCE data, you will
        // probably want to copy that to some other data structure if
        // you want to iterate mutliple times over the data.  

        ArrayList<WebTrafficRecord> myArray = new ArrayList<WebTrafficRecord>();
        //fill in the arraylist first
        for(WebTrafficRecord wtr: values)
        {
            myArray.add(new WebTrafficRecord(wtr));
        }


        for(int i=0; i<myArray.size(); i++)
        {
            //if it's a request:
            if(myArray.get(i).getUserName==Null)
            {
                WebTrafficRecord request=myArray.get(i);
                for(int j=0; j<myArray.size(); j++)
                {
                    WebTrafficRecord reply=myArray.get(j);
                    if(reply.getCookie()==Null  && (reply.getTimestamp()<=request.getTimestamp()-10 || request.getTimestamp()<=reply.getTimestamp()-10) && i!=j)
                    {
                     ctxt.write( RequestReplyMatch( request, reply) , NullWriteable);
                    }
                }
        }
        }
        // ctxt.write should be RequestReplyMatch and a NullWriteable
    }
}
