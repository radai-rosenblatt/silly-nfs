package net.radai.sillynfs;

import org.dcache.nfs.status.NotSyncException;
import org.dcache.nfs.v3.HimeraNfsUtils;
import org.dcache.nfs.v3.xdr.sattr3;
import org.dcache.nfs.v3.xdr.sattrguard3;

/**
 * Created on 13/10/2014.
 */
public class MutableInodeUpdateStrategy implements InodeUpdateStrategy {
    @Override
    public SillyInode sattr(long inodeNumber, SillyInode inode, sattrguard3 guard, sattr3 newAttrs) throws NotSyncException {
        if (guard.check) {
            long expected = HimeraNfsUtils.convertTimestamp(guard.obj_ctime);
            if (inode.getChanceTime() != expected) {
                throw new NotSyncException();
            }
        }
        return inode.setAttrs(newAttrs);
    }
}
