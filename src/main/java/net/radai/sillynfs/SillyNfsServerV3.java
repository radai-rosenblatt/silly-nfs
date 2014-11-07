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

import com.google.common.util.concurrent.Striped;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.NodeFactory;
import com.googlecode.concurrenttrees.radix.node.concrete.SmartArrayBasedNodeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.status.BadHandleException;
import org.dcache.nfs.status.ExistException;
import org.dcache.nfs.status.InvalException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.NotDirException;
import org.dcache.nfs.status.NotEmptyException;
import org.dcache.nfs.status.NotSuppException;
import org.dcache.nfs.v3.HimeraNfsUtils;
import org.dcache.nfs.v3.xdr.ACCESS3args;
import org.dcache.nfs.v3.xdr.ACCESS3res;
import org.dcache.nfs.v3.xdr.ACCESS3resfail;
import org.dcache.nfs.v3.xdr.ACCESS3resok;
import org.dcache.nfs.v3.xdr.COMMIT3args;
import org.dcache.nfs.v3.xdr.COMMIT3res;
import org.dcache.nfs.v3.xdr.CREATE3args;
import org.dcache.nfs.v3.xdr.CREATE3res;
import org.dcache.nfs.v3.xdr.CREATE3resfail;
import org.dcache.nfs.v3.xdr.CREATE3resok;
import org.dcache.nfs.v3.xdr.FSINFO3args;
import org.dcache.nfs.v3.xdr.FSINFO3res;
import org.dcache.nfs.v3.xdr.FSSTAT3args;
import org.dcache.nfs.v3.xdr.FSSTAT3res;
import org.dcache.nfs.v3.xdr.GETATTR3args;
import org.dcache.nfs.v3.xdr.GETATTR3res;
import org.dcache.nfs.v3.xdr.GETATTR3resok;
import org.dcache.nfs.v3.xdr.LINK3args;
import org.dcache.nfs.v3.xdr.LINK3res;
import org.dcache.nfs.v3.xdr.LOOKUP3args;
import org.dcache.nfs.v3.xdr.LOOKUP3res;
import org.dcache.nfs.v3.xdr.LOOKUP3resfail;
import org.dcache.nfs.v3.xdr.LOOKUP3resok;
import org.dcache.nfs.v3.xdr.MKDIR3args;
import org.dcache.nfs.v3.xdr.MKDIR3res;
import org.dcache.nfs.v3.xdr.MKDIR3resfail;
import org.dcache.nfs.v3.xdr.MKDIR3resok;
import org.dcache.nfs.v3.xdr.MKNOD3args;
import org.dcache.nfs.v3.xdr.MKNOD3res;
import org.dcache.nfs.v3.xdr.MKNOD3resfail;
import org.dcache.nfs.v3.xdr.PATHCONF3args;
import org.dcache.nfs.v3.xdr.PATHCONF3res;
import org.dcache.nfs.v3.xdr.READ3args;
import org.dcache.nfs.v3.xdr.READ3res;
import org.dcache.nfs.v3.xdr.READ3resfail;
import org.dcache.nfs.v3.xdr.READ3resok;
import org.dcache.nfs.v3.xdr.READDIR3args;
import org.dcache.nfs.v3.xdr.READDIR3res;
import org.dcache.nfs.v3.xdr.READDIRPLUS3args;
import org.dcache.nfs.v3.xdr.READDIRPLUS3res;
import org.dcache.nfs.v3.xdr.READLINK3args;
import org.dcache.nfs.v3.xdr.READLINK3res;
import org.dcache.nfs.v3.xdr.READLINK3resfail;
import org.dcache.nfs.v3.xdr.REMOVE3args;
import org.dcache.nfs.v3.xdr.REMOVE3res;
import org.dcache.nfs.v3.xdr.REMOVE3resfail;
import org.dcache.nfs.v3.xdr.REMOVE3resok;
import org.dcache.nfs.v3.xdr.RENAME3args;
import org.dcache.nfs.v3.xdr.RENAME3res;
import org.dcache.nfs.v3.xdr.RMDIR3args;
import org.dcache.nfs.v3.xdr.RMDIR3res;
import org.dcache.nfs.v3.xdr.RMDIR3resfail;
import org.dcache.nfs.v3.xdr.RMDIR3resok;
import org.dcache.nfs.v3.xdr.SETATTR3args;
import org.dcache.nfs.v3.xdr.SETATTR3res;
import org.dcache.nfs.v3.xdr.SETATTR3resfail;
import org.dcache.nfs.v3.xdr.SETATTR3resok;
import org.dcache.nfs.v3.xdr.SYMLINK3args;
import org.dcache.nfs.v3.xdr.SYMLINK3res;
import org.dcache.nfs.v3.xdr.SYMLINK3resfail;
import org.dcache.nfs.v3.xdr.WRITE3args;
import org.dcache.nfs.v3.xdr.WRITE3res;
import org.dcache.nfs.v3.xdr.WRITE3resfail;
import org.dcache.nfs.v3.xdr.WRITE3resok;
import org.dcache.nfs.v3.xdr.count3;
import org.dcache.nfs.v3.xdr.createmode3;
import org.dcache.nfs.v3.xdr.fattr3;
import org.dcache.nfs.v3.xdr.fileid3;
import org.dcache.nfs.v3.xdr.gid3;
import org.dcache.nfs.v3.xdr.mode3;
import org.dcache.nfs.v3.xdr.nfs3_prot;
import org.dcache.nfs.v3.xdr.nfs3_protServerStub;
import org.dcache.nfs.v3.xdr.nfs_fh3;
import org.dcache.nfs.v3.xdr.post_op_attr;
import org.dcache.nfs.v3.xdr.post_op_fh3;
import org.dcache.nfs.v3.xdr.pre_op_attr;
import org.dcache.nfs.v3.xdr.sattr3;
import org.dcache.nfs.v3.xdr.size3;
import org.dcache.nfs.v3.xdr.specdata3;
import org.dcache.nfs.v3.xdr.stable_how;
import org.dcache.nfs.v3.xdr.time_how;
import org.dcache.nfs.v3.xdr.uid3;
import org.dcache.nfs.v3.xdr.uint32;
import org.dcache.nfs.v3.xdr.uint64;
import org.dcache.nfs.v3.xdr.wcc_attr;
import org.dcache.nfs.v3.xdr.wcc_data;
import org.dcache.nfs.v3.xdr.writeverf3;
import org.dcache.nfs.vfs.Stat;
import org.dcache.xdr.RpcCall;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import static org.dcache.nfs.v3.HimeraNfsUtils.defaultWccData;
import static org.dcache.nfs.v3.NameUtils.checkFilename;

/**
 * simplified version of NfsServerV3:
 * no sym/hard links
 * no permission checks
 * no caches
 * single file system root
 */
public class SillyNfsServerV3 extends nfs3_protServerStub {
    private static final Logger logger = LogManager.getLogger(SillyNfsServerV3.class);

    private AtomicLong inodeSequence = new AtomicLong(0);
    private Striped<Lock> locks = Striped.lock(64); //TODO - function of #cores
    private NonBlockingHashMapLong<SillyInode> inodeTable = new NonBlockingHashMapLong<>(false);
    private NonBlockingHashMapLong<ConcurrentRadixTree<Long>> directoryTable = new NonBlockingHashMapLong<>();
    private NodeFactory directoryNodeFactory = new SmartArrayBasedNodeFactory();
    private BlobStore blobStore = new HeapBlobStore();
    private InodeUpdateStrategy inodeUpdateStrategy = new MutableInodeUpdateStrategy();
    private NameValidator nameValidator = NameValidator.LOOSE;
    private writeverf3 writeVerifier = generateInstanceWriteVerifier();

    public SillyNfsServerV3() {

    }

    private static writeverf3 generateInstanceWriteVerifier() {
        //in theory we might start, crash, and start again within the same millisecond. dont care.
        writeverf3 verf = new writeverf3();
        verf.value = new byte[nfs3_prot.NFS3_WRITEVERFSIZE];
        ByteBuffer buf = ByteBuffer.wrap(verf.value);
        buf.putLong(System.currentTimeMillis());
        return verf;
    }

    @Override
    public void NFSPROC3_NULL_3(RpcCall call$) {
        //nop
    }

    @Override
    public GETATTR3res NFSPROC3_GETATTR_3(RpcCall call$, GETATTR3args arg1) {
        GETATTR3res res = new GETATTR3res();

        try {
            SillyInodeReference inodeRef = new SillyInodeReference(arg1.object.data);
            SillyInode inode = resolve(inodeRef);

            res.status = nfsstat.NFS_OK;
            res.resok = new GETATTR3resok();
            res.resok.obj_attributes = toAttr(inodeRef.getInodeNumber(), inode);

        } catch (ChimeraNFSException e) {
            res.status = e.getStatus();
        } catch (Exception e) {
            logger.error("GETATTR", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }

        return res;
    }

    @Override
    public SETATTR3res NFSPROC3_SETATTR_3(RpcCall call$, SETATTR3args arg1) {
        SETATTR3res res = new SETATTR3res();

        try {
            SillyInodeReference inodeRef = new SillyInodeReference(arg1.object.data);
            SillyInode inode = resolve(inodeRef);

            res.status = nfsstat.NFS_OK;
            res.resok = new SETATTR3resok();
            res.resok.obj_wcc = new wcc_data();
            res.resok.obj_wcc.before = toPreAttr(inode);

            long sizeBefore = inode.getSize();
            inode = inodeUpdateStrategy.sattr(inodeRef.getInodeNumber(), inode, arg1.guard, arg1.new_attributes);
            //its possible that size changes arent due to setattr ...
            if (arg1.new_attributes.size.set_it && sizeBefore != inode.getSize()) {
                //TODO - truncate
            }

            res.resok.obj_wcc.after = toPostOp(inodeRef.getInodeNumber(), inode);

        } catch (ChimeraNFSException e) {
            res.status = e.getStatus();
            res.resfail = CONST_SETATTR_FAIL_RES;
        } catch (Exception e) {
            logger.error("SETATTR", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.resfail = CONST_SETATTR_FAIL_RES;
        }

        return res;
    }

    @Override
    public LOOKUP3res NFSPROC3_LOOKUP_3(RpcCall call$, LOOKUP3args arg1) {
        LOOKUP3res res = new LOOKUP3res();

        try {
            SillyInodeReference parentInodeRef = new SillyInodeReference(arg1.what.dir.data);
            long parentInodeNumber = parentInodeRef.getInodeNumber();
            String name = arg1.what.name.value;
            nameValidator.checkName(name);
            SillyInode parentInode = resolve(parentInodeRef);
            if (!parentInode.isDirectory()) {
                throw new NotDirException("inode #" + parentInodeNumber + " is not a directory");
            }
            SillyInode childInode;
            long childInodeNumber;
            //noinspection IfCanBeSwitch
            if (name.equals(".")) {
                childInodeNumber = parentInodeNumber;
                childInode = parentInode;
            } else if (name.equals("..")) {
                childInodeNumber = parentInode.getParentInodeNumber();
                childInode = resolve(childInodeNumber);
            } else {
                ConcurrentRadixTree<Long> children = directoryTable.get(parentInodeNumber);
                if (children == null) {
                    throw new NoEntException(); //empty directory
                }
                Long childInodeNumberLong = children.getValueForExactKey(name);
                if (childInodeNumberLong == null) {
                    throw new NoEntException();
                }
                childInodeNumber = childInodeNumberLong;
                childInode = resolve(childInodeNumber);
            }

            res.status = nfsstat.NFS_OK;
            res.resok = new LOOKUP3resok();
            res.resok.object = toHandle(childInodeNumber);
            res.resok.obj_attributes = toPostOp(childInodeNumber, childInode);
            res.resok.dir_attributes = toPostOp(parentInodeNumber, parentInode);

        } catch (ChimeraNFSException e) {
            res.status = e.getStatus();
            res.resfail = CONST_LOOKUP_FAIL_RES;
        } catch (Exception e) {
            logger.error("LOOKUP", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.resfail = CONST_LOOKUP_FAIL_RES;
        }

        return res;
    }

    @Override
    public ACCESS3res NFSPROC3_ACCESS_3(RpcCall call$, ACCESS3args arg1) {
        ACCESS3res res = new ACCESS3res();

        try {
            SillyInodeReference inodeRef = new SillyInodeReference(arg1.object.data);
            SillyInode inode = resolve(inodeRef);

            res.status = nfsstat.NFS_OK;
            res.resok = new ACCESS3resok();

            res.resok.obj_attributes = toPostOp(inodeRef.getInodeNumber(), inode);
            res.resok.access = new uint32(arg1.access.value); //allow everything they ask

        } catch (ChimeraNFSException e) {
            res.status = e.getStatus();
            res.resfail = CONST_ACCESS_FAIL_RES;
        } catch (Exception e) {
            logger.error("ACCESS", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.resfail = CONST_ACCESS_FAIL_RES;
        }

        return res;
    }

    @Override
    public READLINK3res NFSPROC3_READLINK_3(RpcCall call$, READLINK3args arg1) {
        READLINK3res res = new READLINK3res();

        try {
            SillyInodeReference inodeRef = new SillyInodeReference(arg1.symlink.data);
            SillyInode inode = resolve(inodeRef);
            if (!inode.isLink()) {
                throw new InvalException("inode #" + inodeRef.getInodeNumber() + " is not a symlink");
            }

            throw new NotSuppException("symlinks not implemented");

        } catch (ChimeraNFSException e) {
            res.status = e.getStatus();
            res.resfail = CONST_READLINK_FAIL_RES;
        } catch (Exception e) {
            logger.error("READLINK", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.resfail = CONST_READLINK_FAIL_RES;
        }

        return res;
    }

    @Override
    public READ3res NFSPROC3_READ_3(RpcCall call$, READ3args arg1) {
        READ3res res = new READ3res();

        try {
            SillyInodeReference inodeRef = new SillyInodeReference(arg1.file.data);
            SillyInode inode = resolve(inodeRef);
            long offset = arg1.offset.value.value;
            int count = arg1.count.value.value;
            byte[] data = new byte[count];

            int bytesRead = blobStore.read(inodeRef.getInodeNumber(), data, offset, count);
            //TODO - update timestamps

            res.resok = new READ3resok();
            res.resok.data = data;
            res.resok.count = new count3(new uint32(bytesRead));
            res.resok.eof = bytesRead + offset == inode.getSize();
            res.resok.file_attributes = toPostOp(inodeRef.getInodeNumber(), inode);

        } catch (ChimeraNFSException e) {
            res.status = e.getStatus();
            res.resfail = CONST_READ_FAIL_RES;
        } catch (Exception e) {
            logger.error("READ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.resfail = CONST_READ_FAIL_RES;
        }

        return res;
    }

    @Override
    public WRITE3res NFSPROC3_WRITE_3(RpcCall call$, WRITE3args arg1) {
        WRITE3res res = new WRITE3res();

        try {
            SillyInodeReference inodeRef = new SillyInodeReference(arg1.file.data);
            SillyInode inode = resolve(inodeRef);
            long offset = arg1.offset.value.value;
            int count = arg1.count.value.value;
            byte[] data = arg1.data;

            res.resok = new WRITE3resok();
            res.status = nfsstat.NFS_OK;
            res.resok.file_wcc = new wcc_data();
            res.resok.file_wcc.before = toPreAttr(inode);

            int bytesWritten = blobStore.write(inodeRef.getInodeNumber(), data, offset, count);
            //TODO - update timestamps and size

            res.resok.count = new count3(new uint32(bytesWritten));
            res.resok.committed = stable_how.FILE_SYNC; //which is of course a lie
            res.resok.file_wcc.after = toPostOp(inodeRef.getInodeNumber(), inode);
            res.resok.verf = writeVerifier;

        } catch (ChimeraNFSException hne) {
            res.status = hne.getStatus();
            res.resfail = CONST_WRITE_FAIL_RES;
        } catch (Exception e) {
            logger.error("WRITE", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            res.resfail = CONST_WRITE_FAIL_RES;
        }

        return res;
    }

    @Override
    public CREATE3res NFSPROC3_CREATE_3(RpcCall call$, CREATE3args arg1) {
        CREATE3res res = new CREATE3res();

        long parentInodeNumber = -1;
        SillyInode parentInode = null;
        pre_op_attr parentPre = null;
        try {
            SillyInodeReference parentRef = new SillyInodeReference(arg1.where.dir.data);
            parentInodeNumber = parentRef.getInodeNumber();
            parentInode = resolve(parentRef);
            if (!parentInode.isDirectory()) {
                throw new NotDirException("inode #" + parentInodeNumber + " is not a directory");
            }
            parentPre = toPreAttr(parentInode);
            long modCountBefore = parentInode.getModCount();

            //check names after we resolve parent so that we could fill in wcc data on failure result
            String path = arg1.where.name.value;
            checkFilename(path);
            if (path.equals(".") || path.equals("..")) {
                throw new ExistException();
            }

            ConcurrentRadixTree<Long> parentDirectoryEntry = resolveDirectory(parentInodeNumber, true);

            long serverTime = System.currentTimeMillis();
            int mode;
            int uid;
            int gid;
            long size;
            long accessTime;
            long modificationTime;

            int creationMode = arg1.how.mode;
            switch (creationMode) {
                case createmode3.UNCHECKED:
                case createmode3.GUARDED:
                    sattr3 creationAttributes = arg1.how.obj_attributes;
                    //noinspection OctalInteger
                    mode = creationAttributes.mode.set_it ? creationAttributes.mode.mode.value.value&Stat.S_PERMS : 0644;
                    uid = creationAttributes.uid.set_it ? creationAttributes.uid.uid.value.value : 0; //TODO - use caller
                    gid = creationAttributes.gid.set_it ? creationAttributes.gid.gid.value.value : 0; //TODO - use caller
                    size = creationAttributes.size.set_it ? creationAttributes.size.size.value.value : 0;
                    accessTime = creationAttributes.atime.set_it == time_how.SET_TO_CLIENT_TIME ? HimeraNfsUtils.convertTimestamp(creationAttributes.atime.atime) : serverTime;
                    modificationTime = creationAttributes.mtime.set_it == time_how.SET_TO_CLIENT_TIME ? HimeraNfsUtils.convertTimestamp(creationAttributes.mtime.mtime) : serverTime;
                    break;
                case createmode3.EXCLUSIVE:
                    throw new NotSuppException("exclusive file creation not implemented yet");
                default:
                    throw new IllegalStateException();
            }

            long newInodeNumber;
            SillyInode newInode;

            Lock parentLock = locks.get(parentInodeNumber);
            parentLock.lock();
            try {
                long modCountNow = parentInode.getModCount();
                if (modCountNow != modCountBefore) {
                    //TODO - recheck invariants
                    throw new UnsupportedOperationException("TBD");
                }

                //optimistic approach - create the new inode, try inserting it.
                newInodeNumber = inodeSequence.incrementAndGet();
                newInode = createInode(newInodeNumber, parentInodeNumber, mode, Stat.Type.REGULAR, uid, gid, size, accessTime, modificationTime);

                Long existingInodeNumber = parentDirectoryEntry.putIfAbsent(path, newInodeNumber);
                if (existingInodeNumber != null) {
                    //optimistic failure
                    switch (creationMode) {
                        case createmode3.UNCHECKED:
                            //bonus points - attempt to reclaim inode number
                            inodeSequence.compareAndSet(newInodeNumber, newInodeNumber-1);
                            //TODO - apply creationAttributes to existing inode in this case?
                            newInode = inodeTable.get(existingInodeNumber);
                            newInodeNumber = existingInodeNumber;
                            break;
                        case createmode3.GUARDED:
                            //bonus points - attempt to reclaim inode number
                            inodeSequence.compareAndSet(newInodeNumber, newInodeNumber-1);
                            throw new ExistException();
                        case createmode3.EXCLUSIVE:
                            throw new NotSuppException("exclusive file creation not implemented yet");
                    }
                } else {
                    //optimistic success
                    inodeTable.put(newInodeNumber, newInode);
                }

                //TODO - update timestamps, size and modCount on parent

            } finally {
                parentLock.unlock();
            }

            res.status = nfsstat.NFS_OK;
            res.resok = new CREATE3resok();
            res.resok.obj_attributes = toPostOp(newInodeNumber, newInode);
            res.resok.obj = toPostOpHandle(newInodeNumber);
            res.resok.dir_wcc = new wcc_data();
            res.resok.dir_wcc.before = parentPre;
            res.resok.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);

        } catch (ChimeraNFSException hne) {
            res.status = hne.getStatus();
            if (parentPre == null) {
                res.resfail = CONST_CREATE_FAIL_RES;
            } else {
                res.resfail = new CREATE3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        } catch (Exception e) {
            logger.error("create", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            if (parentPre == null) {
                res.resfail = CONST_CREATE_FAIL_RES;
            } else {
                res.resfail = new CREATE3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        }

        return res;
    }

    @Override
    public MKDIR3res NFSPROC3_MKDIR_3(RpcCall call$, MKDIR3args arg1) {
        MKDIR3res res = new MKDIR3res();

        long parentInodeNumber = -1;
        SillyInode parentInode = null;
        pre_op_attr parentPre = null;
        try {
            SillyInodeReference parentRef = new SillyInodeReference(arg1.where.dir.data);
            parentInodeNumber = parentRef.getInodeNumber();
            parentInode = resolve(parentRef);
            if (!parentInode.isDirectory()) {
                throw new NotDirException("inode #" + parentInodeNumber + " is not a directory");
            }
            parentPre = toPreAttr(parentInode);
            long modCountBefore = parentInode.getModCount();

            //check names after we resolve parent so that we could fill in wcc data on failure result
            String path = arg1.where.name.value;
            checkFilename(path);
            if (path.equals(".") || path.equals("..")) {
                throw new ExistException();
            }

            ConcurrentRadixTree<Long> parentDirectoryEntry = resolveDirectory(parentInodeNumber, true);

            sattr3 creationAttributes = arg1.attributes;
            long serverTime = System.currentTimeMillis();
            //noinspection OctalInteger
            int mode = creationAttributes.mode.set_it ? creationAttributes.mode.mode.value.value&Stat.S_PERMS : 0644;
            int uid = creationAttributes.uid.set_it ? creationAttributes.uid.uid.value.value : 0; //TODO - use caller
            int gid = creationAttributes.gid.set_it ? creationAttributes.gid.gid.value.value : 0; //TODO - use caller
            long size = 0; //dir size == number of children, created empty
            long accessTime = creationAttributes.atime.set_it == time_how.SET_TO_CLIENT_TIME ? HimeraNfsUtils.convertTimestamp(creationAttributes.atime.atime) : serverTime;
            long modificationTime = creationAttributes.mtime.set_it == time_how.SET_TO_CLIENT_TIME ? HimeraNfsUtils.convertTimestamp(creationAttributes.mtime.mtime) : serverTime;

            long newInodeNumber;
            SillyInode newInode;

            Lock parentLock = locks.get(parentInodeNumber);
            parentLock.lock();
            try {
                long modCountNow = parentInode.getModCount();
                if (modCountNow != modCountBefore) {
                    //TODO - recheck invariants
                    throw new UnsupportedOperationException("TBD");
                }

                //optimistic approach - create the new inode, try inserting it.
                newInodeNumber = inodeSequence.incrementAndGet();
                newInode = createInode(newInodeNumber, parentInodeNumber, mode, Stat.Type.DIRECTORY, uid, gid, size, accessTime, modificationTime);
                Long existingInodeNumber = parentDirectoryEntry.putIfAbsent(path, newInodeNumber);
                if (existingInodeNumber != null) {
                    //optimistic failure
                    //bonus points - attempt to reclaim inode number
                    inodeSequence.compareAndSet(newInodeNumber, newInodeNumber - 1);
                    throw new ExistException();
                } else {
                    //optimistic success
                    inodeTable.put(newInodeNumber, newInode);
                }

                //TODO - update timestamps size and nlinks on parent

            } finally {
                parentLock.unlock();
            }

            res.status = nfsstat.NFS_OK;
            res.resok = new MKDIR3resok();
            res.resok.obj_attributes = toPostOp(newInodeNumber, newInode);
            res.resok.obj = toPostOpHandle(newInodeNumber);
            res.resok.dir_wcc = new wcc_data();
            res.resok.dir_wcc.before = parentPre;
            res.resok.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);

        } catch (ChimeraNFSException hne) {
            res.status = hne.getStatus();
            if (parentPre == null) {
                res.resfail = CONST_MKDIR_FAIL_RES;
            } else {
                res.resfail = new MKDIR3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        } catch (Exception e) {
            logger.error("mkdir", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            if (parentPre == null) {
                res.resfail = CONST_MKDIR_FAIL_RES;
            } else {
                res.resfail = new MKDIR3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        }

        return res;
    }

    @Override
    public SYMLINK3res NFSPROC3_SYMLINK_3(RpcCall call$, SYMLINK3args arg1) {
        SYMLINK3res res = new SYMLINK3res();

        long parentInodeNumber = -1;
        SillyInode parentInode = null;
        pre_op_attr parentPre = null;
        try {
            SillyInodeReference parentRef = new SillyInodeReference(arg1.where.dir.data);
            parentInodeNumber = parentRef.getInodeNumber();
            parentInode = resolve(parentRef);
            parentPre = toPreAttr(parentInode);

            //check names after we resolve parent so that we could fill in wcc data on failure result
            String path = arg1.where.name.value;
            checkFilename(path);
            if (path.equals(".") || path.equals("..")) {
                throw new ExistException();
            }

            String symlinkTargetPath = arg1.symlink.symlink_data.value;
            sattr3 creationAttributes = arg1.symlink.symlink_attributes;
            long serverTime = System.currentTimeMillis();
            //noinspection OctalInteger
            int mode = creationAttributes.mode.set_it ? creationAttributes.mode.mode.value.value&Stat.S_PERMS : 0644;
            int uid = creationAttributes.uid.set_it ? creationAttributes.uid.uid.value.value : 0; //TODO - use caller
            int gid = creationAttributes.gid.set_it ? creationAttributes.gid.gid.value.value : 0; //TODO - use caller
            long size = 0; //symlinks have no size
            long accessTime = creationAttributes.atime.set_it == time_how.SET_TO_CLIENT_TIME ? HimeraNfsUtils.convertTimestamp(creationAttributes.atime.atime) : serverTime;
            long modificationTime = creationAttributes.mtime.set_it == time_how.SET_TO_CLIENT_TIME ? HimeraNfsUtils.convertTimestamp(creationAttributes.mtime.mtime) : serverTime;

            //TODO - support this
            throw new NotSuppException("symlinks not supported yet");

        } catch (ChimeraNFSException hne) {
            res.status = hne.getStatus();
            if (parentPre == null) {
                res.resfail = CONST_SYMLINK_FAIL_RES;
            } else {
                res.resfail = new SYMLINK3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        } catch (Exception e) {
            logger.error("symlink", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            if (parentPre == null) {
                res.resfail = CONST_SYMLINK_FAIL_RES;
            } else {
                res.resfail = new SYMLINK3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        }

        return res;
    }

    @Override
    public MKNOD3res NFSPROC3_MKNOD_3(RpcCall call$, MKNOD3args arg1) {
        //TODO - support this?!
        MKNOD3res res = new MKNOD3res();
        res.status = nfsstat.NFSERR_NOTSUPP;
        res.resfail = CONST_MKNOD_FAIL_RES;
        return res;
    }

    @Override
    public REMOVE3res NFSPROC3_REMOVE_3(RpcCall call$, REMOVE3args arg1) {
        REMOVE3res res = new REMOVE3res();

        long parentInodeNumber = -1;
        SillyInode parentInode = null;
        pre_op_attr parentPre = null;
        try {
            SillyInodeReference parentRef = new SillyInodeReference(arg1.object.dir.data);
            parentInodeNumber = parentRef.getInodeNumber();
            parentInode = resolve(parentRef);
            if (!parentInode.isDirectory()) {
                throw new NotDirException("inode #" + parentInodeNumber + " is not a directory");
            }
            parentPre = toPreAttr(parentInode);
            long modCountBefore = parentInode.getModCount();

            String name = arg1.object.name.value; //dont bother checking validity. worst-case it wont be found
            ConcurrentRadixTree<Long> parentDirectoryEntry = resolveDirectory(parentInodeNumber, false);
            Long childInodeNumber = parentDirectoryEntry == null ? null : parentDirectoryEntry.getValueForExactKey(name);
            if (childInodeNumber == null) {
                throw new NoEntException();
            }

            Lock parentLock = locks.get(parentInodeNumber);
            parentLock.lock();
            try {
                long modCountNow = parentInode.getModCount();
                if (modCountNow != modCountBefore) {
                    //TODO - recheck invariants
                    throw new UnsupportedOperationException("TBD");
                }

                inodeTable.remove(childInodeNumber.longValue());

                //TODO - update timestamps and size on parent

                if (parentInode.getSize()==0) {
                    //last child removed
                    assert (parentDirectoryEntry.size()==0);
                    if (parentDirectoryEntry != directoryTable.remove(parentInodeNumber)) {
                        throw new IllegalStateException("should never happen");
                    }
                }

            } finally {
                parentLock.unlock();
            }

            res.status = nfsstat.NFS_OK;
            res.resok = new REMOVE3resok();
            res.resok.dir_wcc = new wcc_data();
            res.resok.dir_wcc.before = parentPre;
            res.resok.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);

        } catch (ChimeraNFSException hne) {
            res.status = hne.getStatus();
            if (parentPre == null) {
                res.resfail = CONST_REMOVE_FAIL_RES;
            } else {
                res.resfail = new REMOVE3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        } catch (Exception e) {
            logger.error("symlink", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            if (parentPre == null) {
                res.resfail = CONST_REMOVE_FAIL_RES;
            } else {
                res.resfail = new REMOVE3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        }

        return res;
    }

    @Override
    public RMDIR3res NFSPROC3_RMDIR_3(RpcCall call$, RMDIR3args arg1) {
        RMDIR3res res = new RMDIR3res();

        long parentInodeNumber = -1;
        SillyInode parentInode = null;
        pre_op_attr parentPre = null;
        try {
            SillyInodeReference parentRef = new SillyInodeReference(arg1.object.dir.data);
            parentInodeNumber = parentRef.getInodeNumber();
            parentInode = resolve(parentRef);
            if (!parentInode.isDirectory()) {
                throw new NotDirException("inode #" + parentInodeNumber + " is not a directory");
            }
            parentPre = toPreAttr(parentInode);

            String name = arg1.object.name.value; //dont bother checking validity. worst-case it wont be found
            if (name.equals(".")) {
                //TODO - maybe treat this as an attempt to delete parent?
                throw new InvalException();
            }
            if (name.equals("..")) {
                throw new NotEmptyException(); //parent exists, so grandparent is by definition not empty.
            }
            ConcurrentRadixTree<Long> parentDirectoryEntry = resolveDirectory(parentInodeNumber, false);
            if (parentDirectoryEntry == null) {
                throw new NoEntException();
            }
            Long childInodeNumber = parentDirectoryEntry.getValueForExactKey(name);
            if (childInodeNumber == null) {
                throw new NoEntException();
            }
            long childInodeNumberPrimitive = childInodeNumber;
            SillyInode childInode = resolve(childInodeNumberPrimitive);
            if (!childInode.isDirectory()) {
                throw new NotDirException("inode #" + childInodeNumber + " is not a directory");
            }
            long size = childInode.getSize();
            if (size>0) {
                throw new NotEmptyException();
            }

            parentDirectoryEntry.remove(name);
            //TODO - striped lock array for directory operations
            //cant remove parentDirectoryEntry if its empty because we might be in a race with a CREATE somewhere ...
            inodeTable.remove(childInodeNumberPrimitive);
            directoryTable.remove(childInodeNumberPrimitive);

            res.status = nfsstat.NFS_OK;
            res.resok = new RMDIR3resok();
            res.resok.dir_wcc = new wcc_data();
            res.resok.dir_wcc.before = parentPre;
            res.resok.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);

        } catch (ChimeraNFSException hne) {
            res.status = hne.getStatus();
            if (parentPre == null) {
                res.resfail = CONST_RMDIR_FAIL_RES;
            } else {
                res.resfail = new RMDIR3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        } catch (Exception e) {
            logger.error("symlink", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
            if (parentPre == null) {
                res.resfail = CONST_RMDIR_FAIL_RES;
            } else {
                res.resfail = new RMDIR3resfail();
                res.resfail.dir_wcc = new wcc_data();
                res.resfail.dir_wcc.before = parentPre;
                res.resfail.dir_wcc.after = toPostOp(parentInodeNumber, parentInode);
            }
        }

        return res;
    }

    @Override
    public RENAME3res NFSPROC3_RENAME_3(RpcCall call$, RENAME3args arg1) {
        return null;
    }

    @Override
    public LINK3res NFSPROC3_LINK_3(RpcCall call$, LINK3args arg1) {
        return null;
    }

    @Override
    public READDIR3res NFSPROC3_READDIR_3(RpcCall call$, READDIR3args arg1) {
        return null;
    }

    @Override
    public READDIRPLUS3res NFSPROC3_READDIRPLUS_3(RpcCall call$, READDIRPLUS3args arg1) {
        return null;
    }

    @Override
    public FSSTAT3res NFSPROC3_FSSTAT_3(RpcCall call$, FSSTAT3args arg1) {
        return null;
    }

    @Override
    public FSINFO3res NFSPROC3_FSINFO_3(RpcCall call$, FSINFO3args arg1) {
        return null;
    }

    @Override
    public PATHCONF3res NFSPROC3_PATHCONF_3(RpcCall call$, PATHCONF3args arg1) {
        return null;
    }

    @Override
    public COMMIT3res NFSPROC3_COMMIT_3(RpcCall call$, COMMIT3args arg1) {
        return null;
    }

    private SillyInode resolve(byte[] fileHandle) throws BadHandleException, NoEntException {
        return resolve(new SillyInodeReference(fileHandle));
    }

    private SillyInode resolve(SillyInodeReference inodeRef) throws NoEntException {
        return resolve(inodeRef.getInodeNumber());
    }

    private SillyInode resolve(long inodeNumber) throws NoEntException {
        SillyInode inode = inodeTable.get(inodeNumber);
        if (inode == null) {
            throw new NoEntException("inode #" + inodeNumber + " not found");
        }
        return inode;
    }

    private ConcurrentRadixTree<Long> resolveDirectory(long inodeNumber, boolean createIfDoesntExist) {
        ConcurrentRadixTree<Long> parentDirectoryEntry = directoryTable.get(inodeNumber);
        if (parentDirectoryEntry == null && createIfDoesntExist) {
            parentDirectoryEntry = new ConcurrentRadixTree<>(directoryNodeFactory);
            ConcurrentRadixTree<Long> alreadyThere = directoryTable.putIfAbsent(inodeNumber, parentDirectoryEntry);
            return alreadyThere != null ? alreadyThere : parentDirectoryEntry;
        }
        return parentDirectoryEntry;
    }

    private Long resolveChild(long directoryInodeNumber, String name) {
        ConcurrentRadixTree<Long> parentDirectoryEntry = directoryTable.get(directoryInodeNumber);
        if (parentDirectoryEntry == null) {
            return null;
        }
        return parentDirectoryEntry.getValueForExactKey(name);
    }

    private final static specdata3 CONST_DEV = new specdata3();
    private final static uint64 CONST_FS = new uint64(666);
    private final static pre_op_attr CONST_EMPTY_PREOP = new pre_op_attr();
    private final static post_op_attr CONST_EMPTY_POSTOP = new post_op_attr();
    private final static wcc_data CONST_EMPTY_WCC = new wcc_data();
    private final static SETATTR3resfail CONST_SETATTR_FAIL_RES = new SETATTR3resfail();
    private final static LOOKUP3resfail CONST_LOOKUP_FAIL_RES = new LOOKUP3resfail();
    private final static ACCESS3resfail CONST_ACCESS_FAIL_RES = new ACCESS3resfail();
    private final static READLINK3resfail CONST_READLINK_FAIL_RES = new READLINK3resfail();
    private final static READ3resfail CONST_READ_FAIL_RES = new READ3resfail();
    private final static WRITE3resfail CONST_WRITE_FAIL_RES = new WRITE3resfail();
    private final static CREATE3resfail CONST_CREATE_FAIL_RES = new CREATE3resfail();
    private final static MKDIR3resfail CONST_MKDIR_FAIL_RES = new MKDIR3resfail();
    private final static SYMLINK3resfail CONST_SYMLINK_FAIL_RES = new SYMLINK3resfail();
    private final static MKNOD3resfail CONST_MKNOD_FAIL_RES = new MKNOD3resfail();
    private final static REMOVE3resfail CONST_REMOVE_FAIL_RES = new REMOVE3resfail();
    private final static RMDIR3resfail CONST_RMDIR_FAIL_RES = new RMDIR3resfail();

    static {
        CONST_DEV.specdata1 = new uint32(666);
        CONST_DEV.specdata2 = new uint32(666);
        CONST_EMPTY_PREOP.attributes_follow = false;
        CONST_EMPTY_POSTOP.attributes_follow = false;
        CONST_EMPTY_WCC.before = CONST_EMPTY_PREOP;
        CONST_EMPTY_WCC.after = CONST_EMPTY_POSTOP;
        CONST_SETATTR_FAIL_RES.obj_wcc = CONST_EMPTY_WCC;
        CONST_LOOKUP_FAIL_RES.dir_attributes = CONST_EMPTY_POSTOP;
        CONST_ACCESS_FAIL_RES.obj_attributes = CONST_EMPTY_POSTOP;
        CONST_READLINK_FAIL_RES.symlink_attributes = CONST_EMPTY_POSTOP;
        CONST_READ_FAIL_RES.file_attributes = CONST_EMPTY_POSTOP;
        CONST_WRITE_FAIL_RES.file_wcc = CONST_EMPTY_WCC;
        CONST_CREATE_FAIL_RES.dir_wcc = CONST_EMPTY_WCC;
        CONST_MKDIR_FAIL_RES.dir_wcc = CONST_EMPTY_WCC;
        CONST_SYMLINK_FAIL_RES.dir_wcc = CONST_EMPTY_WCC;
        CONST_MKNOD_FAIL_RES.dir_wcc = CONST_EMPTY_WCC;
        CONST_REMOVE_FAIL_RES.dir_wcc = CONST_EMPTY_WCC;
        CONST_RMDIR_FAIL_RES.dir_wcc = CONST_EMPTY_WCC;
    }

    private pre_op_attr toPreAttr(SillyInode inode) {
        pre_op_attr result = new pre_op_attr();
        result.attributes_follow = true;
        result.attributes = new wcc_attr();
        result.attributes.ctime = HimeraNfsUtils.convertTimestamp(inode.getChanceTime());
        result.attributes.mtime = HimeraNfsUtils.convertTimestamp(inode.getModificationTime());
        result.attributes.size = new size3(new uint64(inode.getSize()));
        return result;
    }

    private post_op_attr toPostOp(long inodeNumber, SillyInode inode) {
        post_op_attr result = new post_op_attr();
        result.attributes_follow = true;
        result.attributes = toAttr(inodeNumber, inode);
        return result;
    }

    private post_op_fh3 toPostOpHandle(long inodeNumber) {
        post_op_fh3 result = new post_op_fh3();
        result.handle_follows = true;
        result.handle = toHandle(inodeNumber);
        return result;
    }

    private fattr3 toAttr(long inodeNumber, SillyInode inode) {
        fattr3 result = new fattr3();
        result.type = inode.getType();
        result.mode = new mode3(new uint32(inode.getPermissions()));
        result.nlink = new uint32(inode.getLinkCount());
        result.uid = new uid3(new uint32(inode.getOwnerUserId()));
        result.gid = new gid3(new uint32(inode.getOwnerGroupId()));
        result.size = result.used = new size3(new uint64(inode.getSize()));
        result.rdev = CONST_DEV;
        result.fsid = CONST_FS;
        result.fileid = new fileid3(new uint64(inodeNumber));
        result.atime = HimeraNfsUtils.convertTimestamp(inode.getAccessTime());
        result.mtime = HimeraNfsUtils.convertTimestamp(inode.getModificationTime());
        result.ctime = HimeraNfsUtils.convertTimestamp(inode.getChanceTime());
        return result;
    }

    private nfs_fh3 toHandle(long inodeNumber) {
        nfs_fh3 result = new nfs_fh3();
        result.data = SillyInodeReference.produceFor(inodeNumber);
        return result;
    }

    private SillyInode createInode(long inodeNumber,
                                   long parentInodeNumber,
                                   int mode,
                                   Stat.Type type,
                                   int uid,
                                   int gid,
                                   long size,
                                   long accessTime,
                                   long modificationTime) {
        //TODO - finish this
        return new MutableSillyInode();
    }

}
