import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.io.IOException;
import java.security.MessageDigest;
import org.apache.hadoop.fs.FileSystem;
import java.io.FileOutputStream; //no need?
import java.io.ObjectInputStream;
import org.apache.hadoop.fs.Path;
import java.util.Iterator;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.fs.FSDataInputStream;
import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;

public class TotalFailMapper extends Mapper<LongWritable, Text, WTRKey,
                                            RequestReplyMatch> {

    private MessageDigest messageDigest;
    @Override
    public void setup(Context ctx) throws IOException, InterruptedException{
        // You probably need to do the same setup here you did
        // with the QFD writer
         super.setup(ctx);
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
       // String filename= "qfds/srcIP/srcIP_"+hashString;
        String filename= "qfds/"+line.toString()+"/"+line.toString()+"_"+hashString;
        Path path=new Path(filename);
        QueryFocusedDataSet qfds=null;
        try 
        {
            FileSystem hdfs=FileSystem.get(ctxt.getConfiguration());
            FSDataInputStream inputStream = hdfs.open(path);   //??????????????how to create input stream...
            ObjectInputStream is=new ObjectInputStream(inputStream);
            qfds=(QueryFocusedDataSet)is.readObject();
            inputStream.close();
            is.close();
        } 
        catch (Exception e) 
        { 
            e.printStackTrace(); 
        }        

        //得到这个srcIP的associated 所有的<srcIP, matching>
        Set<RequestReplyMatch> matches=qfds.getMatches();  //what if no match???
        String cook;
        Iterator iterr;
        for(iterr=matches.iterator(); iterr.hasNext();)
        {
            RequestReplyMatch match=(RequestReplyMatch)iterr.next();
            cook= match.getCookie();   //get cookie from each request/reply pair.
            MessageDigest xd = HashUtils.cloneMessageDigest(messageDigest);
            xd.update(cook.getBytes(StandardCharsets.UTF_8));
            hash = xd.digest();
            hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            String cookieHash = DatatypeConverter.printHexBinary(hashBytes);  //generate a hash for each cookie.

            //打开以该cookie 为key的文件，得到<cookie, matching>
            //filename= "qfds/cookie/cookie_"+cookieHash;
            filename= "qfds/"+ cook+"/"+cook +"_"+cookieHash;
            path=new Path(filename);
            QueryFocusedDataSet qfdsc=null;
            try 
            {
                FileSystem hdfsk=FileSystem.get(ctxt.getConfiguration());
                FSDataInputStream inpuStream = hdfsk.open(path);
                ObjectInputStream ks=new ObjectInputStream(inpuStream);
                qfdsc=(QueryFocusedDataSet)ks.readObject();
                inpuStream.close();
                ks.close();
            } 
            catch (Exception e) 
            { 
                e.printStackTrace(); 
            }

            //这里应该只有一个couple，我觉得吧。。？每个cookie 只联系一组<srcIP, destIP>
        Set<RequestReplyMatch> couple=qfdsc.getMatches();
        //context write each request/reply pair associated with each cookie
        Iterator iter;
        for(iter=couple.iterator(); iter.hasNext();)
        {
            RequestReplyMatch record=(RequestReplyMatch)iter.next();

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
}
