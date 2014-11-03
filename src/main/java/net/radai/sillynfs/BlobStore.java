package net.radai.sillynfs;

/**
 * Created on 16/10/2014.
 */
public interface BlobStore {
    int write(long blobId, byte[] data, long offset, int count);
    int read(long blobId, byte[] data, long offset, int count);
}
