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
