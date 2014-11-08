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

package net.radai.sillynfs.util;

import java.util.concurrent.locks.Lock;

/**
 * Created on 08/11/2014.
 */
public class AutoCloseableLocks implements AutoCloseable{
    private final Iterable<Lock> locks;

    public AutoCloseableLocks(Iterable<Lock> locks) {
        this.locks = locks;
        for (Lock lock : this.locks) {
            lock.lock();
        }
    }

    @Override
    public void close() throws Exception {
        for (Lock lock : locks) {
            lock.unlock();
        }
    }
}
