import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class QFDWriterReducer extends Reducer<WTRKey, RequestReplyMatch, NullWritable, NullWritable> {

    @Override
    public void reduce(WTRKey key, Iterable<RequestReplyMatch> values,
                       Context ctxt) throws IOException, InterruptedException {

        // The input will be a WTR key and a set of matches.

        // You will want to open the file named  
        // "qfds/key.getName()/key.getName()_key.getHashBytes()"
        // using the FileSystem interface for Hadoop.

        // EG, if the key's name is srcIP and the hash is 2BBB,
        // the filename should be qfds/srcIP/srcIP_2BBB

        // Some useful functionality:

        // FileSystem.get(ctxt.getConfiguration())
        // gets the interface to the filesysstem
        // new Path(filename) gives a path specification
        // hdfs.create(path, true) will create an
        // output stream pointing to that file   
        // use the reducer for is writing a QueryFocusedDataSet to the filesystem. 
        List<WebTrafficRecord> myArr = new ArrayList<>();
        for (WebTrafficRecord wtr: values) {
            myArr.add(new WebTrafficRecord(wtr));
        }
        Set<QueryFocusedDataSet> qfdsSet = new HashSet();
        for(int k=0; k<myArr.size(); k++)
        {
            qfdsSet.add( myArr.get(k));
        }
        
        String keyName=key.getName();
        String keyHash=key.getHashBytes();
        String filename="qfds/"+keyName+"/"+keyName+"_"+keyHash;
        FileSystem file=FileSystem.get(ctxt.getConfiguration());
        Path path=new Path(filename);
        FSDataOutputStream outputStream = hdfs.create(path);
        ObjectOutputStream oos=new ObjectOutputStream(outputStream);
        oos.writeObject(new QueryFocusedDataSet(keyName, keyHash, qfdsSet));  
        oos.close();
    }
}

