/*
 * Copyright (C) 2014 Radai Rosenblatt
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
 * USA
 */

package net.radai.sillynfs;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.dcache.nfs.status.NotSyncException;
import org.dcache.nfs.v3.HimeraNfsUtils;
import org.dcache.nfs.v3.xdr.sattr3;
import org.dcache.nfs.v3.xdr.sattrguard3;

/**
 * Created on 13/10/2014.
 */
public class ImmutableInodeUpdateStrategy implements InodeUpdateStrategy {
    private final NonBlockingHashMapLong<SillyInode> inodeTable;

    public ImmutableInodeUpdateStrategy(NonBlockingHashMapLong<SillyInode> inodeTable) {
        this.inodeTable = inodeTable;
    }

    @Override
    public SillyInode sattr(long inodeNumber, SillyInode inode, sattrguard3 guard, sattr3 newAttrs) throws NotSyncException {
        if (guard.check) {
            long expected = HimeraNfsUtils.convertTimestamp(guard.obj_ctime);
            if (inode.getChanceTime() != expected) {
                throw new NotSyncException();
            }
        }
        SillyInode newValue = inode.setAttrs(newAttrs);
        boolean success = inodeTable.replace(inodeNumber, inode, newValue);
        while (!success) { //repeat process until we succeed
            if (guard.check) { //ctime must have changed
                throw new NotSyncException();
            }
            newValue = inode.setAttrs(newAttrs);
            success = inodeTable.replace(inodeNumber, inode, newValue);
        }
        return newValue;
    }
}
