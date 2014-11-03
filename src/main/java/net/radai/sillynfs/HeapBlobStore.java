package net.radai.sillynfs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created on 16/10/2014.
 */
public class HeapBlobStore implements BlobStore {
    private static final Logger logger = LogManager.getLogger(SillyNfsServerV3.class);

    private final long totalSize;
    private final int chunkSize;
    private AtomicLong usedGrossSize;
    private AtomicLong usedNetSize;
    private final NonBlockingHashMapLong<HeapBlobEntry> entries = new NonBlockingHashMapLong<>();
    private final ConcurrentLinkedQueue<byte[]> freeChunks = new ConcurrentLinkedQueue<>();

    public HeapBlobStore() {
        this(1024*64, 100*1024*1024); //100MB total in 64k chunks
    }

    public HeapBlobStore(int chunkSizeBytes, long totalSizeBytes) {
        this.chunkSize = chunkSizeBytes;
        int numOfChunks = (int)((totalSizeBytes+(chunkSize-1)) / chunkSize); //round up
        this.totalSize = (long) chunkSize * numOfChunks;
        for (int i=0; i<numOfChunks; i++) {
            freeChunks.add(new byte[chunkSize]);
        }
        this.usedGrossSize = new AtomicLong(0);
        this.usedNetSize = new AtomicLong(0);
    }

    public long getTotalSize() {
        return totalSize;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getNumOfFreeChunks() {
        return freeChunks.size();
    }

    public long getUsedGrossSize() {
        return usedGrossSize.get();
    }

    public long getUsedGrossSize(long blobId) {
        HeapBlobEntry entry = entries.get(blobId);
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        return entry.getTotalSize();
    }

    public long getUsedNetSize() {
        return usedNetSize.get();
    }

    public long getUsedNetSize(long blobId) {
        HeapBlobEntry entry = entries.get(blobId);
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        return entry.getUsedSize();
    }

    @Override
    public int write(long blobId, byte[] data, long offset, int count) {
        logger.debug("write {} {}-{}",blobId, offset, offset+count);
        HeapBlobEntry entry = entries.get(blobId);
        boolean shouldPut = false;
        if (entry == null) {
            entry = new HeapBlobEntry();
            shouldPut = true;
        }

        entry.ensureCapacity(offset+count, freeChunks); //ret val will always be 0

        int bytesWritten = entry.write(data, offset, count);

        if (shouldPut) {
            entries.put(blobId, entry);
        }
        logger.debug("write {} {}-{} complete",blobId, offset, offset+count);

        return bytesWritten;
    }

    @Override
    public int read(long blobId, byte[] data, long offset, int count) {
        logger.debug("read {} {}-{}",blobId, offset, offset+count);
        HeapBlobEntry entry = entries.get(blobId);
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        int actuallyRead = entry.read(data, offset, count);
        logger.debug("read {} {}-{} complete, read {} bytes",blobId, offset, offset+count, actuallyRead);
        return actuallyRead;
    }

    public long remove(long blobId) {
        HeapBlobEntry entry = entries.remove(blobId);
        if (entry == null) {
            return 0;
        }
        //reclaim chunks
        return entry.resizeTo(0, freeChunks);
    }

    public long resizeTo(long blobId, long toSize) {
        if (toSize == 0) {
            return remove(blobId);
        }
        HeapBlobEntry entry = entries.get(blobId);
        if (entry == null) {
            throw new IllegalArgumentException();
        }
        return entry.resizeTo(toSize, freeChunks);
    }

    private class HeapBlobEntry {
        private long usedSize;
        private final ArrayList<byte[]> chunks = new ArrayList<>();

        public long getUsedSize() {
            return usedSize;
        }

        public long getTotalSize() {
            return chunks.size() * chunkSize;
        }

        public int getNumOfChunks() {
            return chunks.size();
        }

        public void ensureExtraChunkCapacity(int expectedNumOfExtraChunks) {
            chunks.ensureCapacity(chunks.size() + expectedNumOfExtraChunks);
        }

        public void addChunk(byte[] newChunk) {
            chunks.add(newChunk);
        }

        public long ensureCapacity(long targetCapacity, ConcurrentLinkedQueue<byte[]> freeChunks) {
            if (chunks.size() * chunkSize < targetCapacity) {
                resizeTo(targetCapacity, freeChunks);
            }
            return 0; //we never free up anything
        }

        /**
         * resizes entry to accommodate (at least) targetSize bytes.
         * this operation may either claim new chunks from the freechunks
         * list or return no-longer-needed chunks to the list.
         * BlobStore fields are updates to reflect any changes in capacity/usage
         * @param targetSize target size in bytes
         * @param freeChunks free chunk list
         * @return number of net bytes freed up, or 0 if none
         */
        public long resizeTo(long targetSize, ConcurrentLinkedQueue<byte[]> freeChunks) {
            if (targetSize == 0) {
                freeChunks.addAll(chunks);
                usedGrossSize.addAndGet(-chunks.size() * (long)chunkSize);
                chunks.clear();
                long prevUsedSize = usedSize;
                usedSize = 0;
                usedNetSize.addAndGet(-prevUsedSize);
                return prevUsedSize;
            }

            int targetNumberOfChunks = (int) ((targetSize + chunkSize - 1) / chunkSize); //round-up division
            int chunksToFree = chunks.size() - targetNumberOfChunks;

            if (chunksToFree > 0) {
                List<byte[]> chunksToReturn = chunks.subList(targetNumberOfChunks, chunks.size());
                freeChunks.addAll(chunksToReturn);
                for (int i=chunks.size()-1; i>=targetNumberOfChunks; i--) {
                    chunks.remove(i);
                }
                usedGrossSize.addAndGet(-chunksToFree * (long)chunkSize);
            } else if (chunksToFree < 0) {
                //chunks to allocate then.
                int chunksToAllocate = -chunksToFree;
                ensureExtraChunkCapacity(targetNumberOfChunks);
                for (int i=0; i<chunksToAllocate; i++) {
                    addChunk(freeChunks.remove());
                }
                usedGrossSize.addAndGet(chunksToAllocate * (long)chunkSize);
            }

            if (usedSize > targetSize) {
                //net used capacity decreased
                long freedUp = usedSize-targetSize;
                usedSize = targetSize;
                usedNetSize.addAndGet(-freedUp);
                return freedUp;
            } else {
                //if its smaller than target it remains unchanged
                return 0;
            }
        }

        /**
         * writes count number of bytes from the given data buffer
         * (from the start of the buffer) to this blob entry, starting
         * at the given offset.
         * <br/>
         * we assume that write() was preceded by resizeTo(). otherwise we'll
         * get an IndexOutOfBounds exception somewhere below
         * @param data input buffer
         * @param offset offset in entry to start writing from
         * @param count number of bytes to write from data
         * @return number of bytes written (should be equal to count if all went ok)
         */
        public int write(byte[] data, long offset, int count) {

            int startChunk = (int)(offset/chunkSize); //trim (round down)

            //handle first chunk separately (more complex due to offset)
            int offsetInFirstChunk = (int) (offset % chunkSize);
            int chunkNumber = startChunk;
            byte[] targetChunk = chunks.get(chunkNumber);
            int availableInFirstChunk = targetChunk.length-offsetInFirstChunk;
            int countInChunk = count > availableInFirstChunk ? availableInFirstChunk : count;
            System.arraycopy(data, 0, targetChunk, offsetInFirstChunk, countInChunk);

            //rest of the chunks (if any) are easier
            int remainingCount = count - countInChunk;
            while (remainingCount > 0) {
                chunkNumber++;
                targetChunk = chunks.get(chunkNumber);
                countInChunk = remainingCount>chunkSize ? chunkSize : remainingCount;
                System.arraycopy(data, count-remainingCount, targetChunk, 0, countInChunk);
                remainingCount -= countInChunk;
            }

            long usedSizePostOp = offset+count;
            if (usedSizePostOp > usedSize) {
                usedNetSize.addAndGet(usedSizePostOp-usedSize);
                usedSize = usedSizePostOp;
            }
            return count;
        }

        public int read(byte[] data, long offset, int count) {

            //make sure read is within bounds
            if (count + offset > usedSize) {
                long fixedCount = usedSize-offset;
                if (fixedCount<=0 || fixedCount>Integer.MAX_VALUE) {
                    return 0;
                }
                count = (int)(fixedCount);
            }

            int startChunk = (int)(offset/chunkSize); //trim (round down)

            //handle first chunk separately (more complex due to offset)
            int offsetInFirstChunk = (int) (offset % chunkSize);
            int chunkNumber = startChunk;
            byte[] sourceChunk = chunks.get(chunkNumber);
            int countInChunk = count;
            int availableInFirstChunk = chunkSize-offsetInFirstChunk;
            if (countInChunk > availableInFirstChunk) {
                countInChunk = availableInFirstChunk;
            }
            System.arraycopy(sourceChunk, offsetInFirstChunk, data, 0, countInChunk);

            //rest of the chunks (if any) are easier
            int remainingCount = count - countInChunk;
            while (remainingCount > 0) {
                chunkNumber++;
                sourceChunk = chunks.get(chunkNumber);
                countInChunk = remainingCount>chunkSize ? chunkSize : remainingCount;
                System.arraycopy(sourceChunk, 0, data, count-remainingCount, countInChunk);
                remainingCount -= countInChunk;
            }

            return count;
        }
    }
}
