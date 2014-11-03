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

import org.dcache.nfs.status.NotSyncException;
import org.dcache.nfs.v3.xdr.sattr3;
import org.dcache.nfs.v3.xdr.sattrguard3;

/**
 * Created on 13/10/2014.
 */
public interface InodeUpdateStrategy {
    SillyInode sattr(long inodeNumber, SillyInode inode, sattrguard3 guard, sattr3 newAttrs) throws NotSyncException;
}
