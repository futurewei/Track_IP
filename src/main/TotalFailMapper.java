import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.io.IOException;
import java.security.MessageDigest;
import org.apache.hadoop.fs.FileSystem;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.nio.file.Path;


public class TotalFailMapper extends Mapper<LongWritable, Text, WTRKey,
                                            RequestReplyMatch> {

    private MessageDigest messageDigest;
    @Override
    public void setup(Context ctx) {
        // You probably need to do the same setup here you did
        // with the QFD writer
         super.setup(ctxt);
        try {
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 is required on all Java platforms, so this
            // should never occur
            throw new RuntimeException("SHA-1 algorithm not available");
        }
        // Now we are adding the salt...
        messageDigest.update(HashUtils.SALT.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void map(LongWritable lineNo, Text line, Context ctxt)
            throws IOException, InterruptedException {

        // The value in the input for the key/value pair is a Tor IP.
        // You need to then query that IP's source QFD to get
        // all cookies from that IP,
        // query the cookie QFDs to get all associated requests
        // which are by those cookies, and store them in a torusers QFD
        MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
        md.update(line.toString().getBytes(StandardCharsets.UTF_8));
        byte[] hash = md.digest();
        byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
        String hashString = DatatypeConverter.printHexBinary(hashBytes);
        String filename= "qfds/srcIP/srcIP_"+hashString;
        Path path=new Path(filename);
        try 
        {
            FileSystem file=FileSystem.get(ctxt.getConfiguration());
            FSDataInputStream inputStream = hdfs.create(path);
            ObjectInputStream is=new ObjectInputStream(inputStream);
            QueryFocusedDataSet qfds=(QueryFocusedDataSet)is.readObject();
            is.close();
        } 
        catch (Exception e) 
        { 
            e.printStackTrace(); 
        }        

        Set<RequestReplyMatch> matches=qfds.getMatches();
        String cook;
        for(iterr=matches.iterator; iterr.hasNext();)
        {
            match=iterr.next();
            cook= match.getCookie();   //get cookie from each request/reply pair.
            MessageDigest xd = HashUtils.cloneMessageDigest(messageDigest);
            xd.update(cook.getBytes(StandardCharsets.UTF_8));
            hash = xd.digest();
            hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            String cookieHash = DatatypeConverter.printHexBinary(hashBytes);  //generate a hash for each cookie.

            filename= "qfds/Cookie/Cookie_"+cookieHash;
            path=new Path(filename);
            try 
            {
                FileSystem file=FileSystem.get(ctxt.getConfiguration());
                FSDataInputStream inpuStream = hdfs.create(path);
                ObjectInputStream ks=new ObjectInputStream(inpuStream);
                QueryFocusedDataSet qfdsc=(QueryFocusedDataSet)ks.readObject();
                ks.close();
            } 
            catch (Exception e) 
            { 
                e.printStackTrace(); 
            }

        Set<RequestReplyMatch> couple=qfdsc.getMatches();
        //context write each request/reply pair associated with each cookie
        for(iter=couple.iterator(); iter.hasNext();)
        {
            record=iter.next();

            MessageDigest hd = HashUtils.cloneMessageDigest(messageDigest);
            hd.update(record.getUserName().getBytes(StandardCharsets.UTF_8));
            hash = hd.digest();
            hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            String hashUser = DatatypeConverter.printHexBinary(hashBytes);

            WTRKey userKey = new WTRKey("torusers", hashUser);
            ctxt.write(userKey, record);   //username
        }
    }
}
