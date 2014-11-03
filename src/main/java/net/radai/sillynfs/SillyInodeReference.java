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
