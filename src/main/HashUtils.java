import java.security.MessageDigest;

// Message digests are secure one-way hash functions that take arbitrary-sized data and output a fixed-length hash value.

public class HashUtils {
    public static final String SALT = "wklvlvdvhfuhwphvvdjhwkdwhyhqwkhqvdzrqwilqg";
    public static final int NUM_HASH_BYTES = 2;

    // Convenience method to circumvent Java's annoying checked exceptions
    public static MessageDigest cloneMessageDigest(MessageDigest digest) {
        try {
            return (MessageDigest) digest.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone MessageDigest");
        }
    }
}
