import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class QFDWriterMapper extends Mapper<RequestReplyMatch,
				     NullWritable, WTRKey,
				     RequestReplyMatch> {
    private MessageDigest messageDigest;


    // For the Query Focused Dataset, we take the key we are using,
    // hash it with a cryptographic hash function, and only use
    // the HashUtils.NUM_HASH_BYTES.

    // We start by creating a messageDigest instance, and preseed it
    // with the salt also contained in HashUtils.  This is so that
    // nobody can predict which names map to which locations
    @Override
    public void setup(Context ctxt) throws IOException, InterruptedException {
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
	public void map(RequestReplyMatch record, NullWritable ignore, Context ctxt)
	        throws IOException, InterruptedException {

        // This is the key mapping function.  You should
        // have the outputs be WTRKey/RequestReplyMatch pairs
        // since this dictates how to go to a particular
        // QFD
                //design a particular hashkey for a particular attribute, such as IP.
        // As an example, to dispatch for the "srcIP", the
        // hash should be

            // MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
            // md.update(record.getSrcIp().getBytes(StandardCharsets.UTF_8));
            // byte[] hash = md.digest();
            // byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            // String hashString = DatatypeConverter.printHexBinary(hashBytes);
            // WTRKey key = new WTRKey("srcIP", hashString);
            // ctxt.write(key, record);

        /*
        Create a copy of the messageDigest variable, which was initialized in the setup method. This represents a hash function.
        Add the bytes of the record's source IP to the hash function object.
        Compute the hash function, which produces an array of bytes.
        Copy this array to a new array, *****but only including the first HashUtils.NUM_HASH_BYTES elements.
        Convert the contents of this new array to a string in hexadecimal notation.
        Create a new WTRKey object.
        Emit a key/value pair to be used in a reducer. The key is the WTRKey that was just created, while the value is the record.
        */

        // You need to do srcIP, destIP, and cookie QFD dispatch, and should
        // be able to use a much more generic structure   ?
       //simply repeat  3 times???

            MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
            md.update(record.getSrcIp().getBytes(StandardCharsets.UTF_8));
            byte[] hash = md.digest();
            byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            String hashString = DatatypeConverter.printHexBinary(hashBytes);
            WTRKey key = new WTRKey("srcIP", hashString);
            ctxt.write(key, record);

            MessageDigest cd = HashUtils.cloneMessageDigest(messageDigest);
            cd.update(record.getDestIp().getBytes(StandardCharsets.UTF_8));
            byte[] hash = cd.digest();
            byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            String hashString = DatatypeConverter.printHexBinary(hashBytes);
            WTRKey key = new WTRKey("DestIP", hashString);
            ctxt.write(key, record);

            MessageDigest hd = HashUtils.cloneMessageDigest(messageDigest);
            hd.update(record.getCookie().getBytes(StandardCharsets.UTF_8));
            byte[] hash = hd.digest();
            byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            String hashString = DatatypeConverter.printHexBinary(hashBytes);
            WTRKey key = new WTRKey("Cookie", hashString);
            ctxt.write(key, record);
    }

}
