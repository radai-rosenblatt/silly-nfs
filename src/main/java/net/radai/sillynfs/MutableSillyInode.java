package net.radai.sillynfs;

import org.dcache.nfs.v3.xdr.sattr3;

/**
 * Created on 09/10/2014.
 */
public class MutableSillyInode implements SillyInode {
    @Override
    public long getModCount() {
        return 0;
    }

    @Override
    public long getParentInodeNumber() {
        return 0;
    }

    @Override
    public int getType() {
        return 0;
    }

    @Override
    public int getPermissions() {
        return 0;
    }

    @Override
    public int getLinkCount() {
        return 0;
    }

    @Override
    public int getOwnerUserId() {
        return 0;
    }

    @Override
    public int getOwnerGroupId() {
        return 0;
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public long getAccessTime() {
        return 0;
    }

    @Override
    public long getModificationTime() {
        return 0;
    }

    @Override
    public long getChanceTime() {
        return 0;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isLink() {
        return false;
    }

    @Override
    public SillyInode setAttrs(sattr3 newAttrs) {
        return null;
    }
}
