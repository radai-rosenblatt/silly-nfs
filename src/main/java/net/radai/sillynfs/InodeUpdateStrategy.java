package net.radai.sillynfs;

import org.dcache.nfs.status.NotSyncException;
import org.dcache.nfs.v3.xdr.sattr3;
import org.dcache.nfs.v3.xdr.sattrguard3;

/**
 * Created on 13/10/2014.
 */
public interface InodeUpdateStrategy {
    SillyInode sattr(long inodeNumber, SillyInode inode, sattrguard3 guard, sattr3 newAttrs) throws NotSyncException;
}
