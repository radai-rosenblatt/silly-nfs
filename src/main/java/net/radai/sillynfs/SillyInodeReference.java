package net.radai.sillynfs;

import org.dcache.nfs.status.BadHandleException;
import org.dcache.utils.Bytes;

import java.nio.ByteBuffer;

/**
 * Created on 09/10/2014.
 */
public class SillyInodeReference {
    //TODO - switch to using Bits + unnsafe?
    private final ByteBuffer data;

    public SillyInodeReference(byte[] blob) throws BadHandleException {
        if (blob==null || blob.length != 8) {
            throw new BadHandleException();
        }
        this.data = ByteBuffer.wrap(blob);
    }

    public static byte[] produceFor(long inodeNumber) {
        byte[] result = new byte[8];
        Bytes.putLong(result, 0, inodeNumber);
        return result;
    }

    public long getInodeNumber() {
        return data.getLong(0);
    }

    public long getModCount() {
        return data.getLong(8);
    }
}
