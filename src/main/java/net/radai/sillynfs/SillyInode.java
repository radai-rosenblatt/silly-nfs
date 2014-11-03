package net.radai.sillynfs;

import org.dcache.nfs.v3.xdr.sattr3;

import java.io.Serializable;

/**
 * Created on 09/10/2014.
 */
public interface SillyInode extends Serializable, Cloneable {

    //data access

    long getModCount();
    long getParentInodeNumber();
    int getType();
    int getPermissions();
    int getLinkCount();
    int getOwnerUserId();
    int getOwnerGroupId();
    long getSize();
    long getAccessTime();
    long getModificationTime();
    long getChanceTime();

    boolean isDirectory();
    boolean isLink();

    //mutators

    SillyInode setAttrs(sattr3 newAttrs);
}
